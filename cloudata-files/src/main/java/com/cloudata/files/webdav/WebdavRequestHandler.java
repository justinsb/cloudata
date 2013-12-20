package com.cloudata.files.webdav;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.files.fs.FsFile;
import com.cloudata.files.fs.FsFileAlreadyExistsException;
import com.cloudata.files.webdav.chunks.ChunkAccumulator;
import com.cloudata.files.webdav.chunks.SimpleChunkAccumulator;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public class WebdavRequestHandler implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(WebdavRequestHandler.class);

    final WebdavContext webdav;

    final ChannelHandlerContext ctx;
    final WebdavRequest request;

    private FsFile file;

    private long started;

    public WebdavRequestHandler(WebdavContext webdav, ChannelHandlerContext ctx, WebdavRequest request) {
        Preconditions.checkNotNull(ctx);

        this.webdav = webdav;
        this.ctx = ctx;
        this.request = request;
    }

    @Override
    public void close() throws IOException {
        long t = System.currentTimeMillis();

        assert started != 0;
        t -= started;

        // if (t < 100) {
        // log.debug("Operation took: " + t + "ms: " + this);
        // } else {
        // log.info("Operation took: " + t + "ms: " + this);
        // }

        if (file != null) {
            file.close();
            file = null;
        }

        if (request != null) {
            request.close();
        }
    }

    public ListenableFuture<HttpObject> processRequest() throws Exception {
        this.started = System.currentTimeMillis();

        MethodHandler handler;

        if (request.getMethod().name().equals("LOCK")) {
            handler = new LockHandler(this);
        } else if (request.getMethod().name().equals("UNLOCK")) {
            handler = new UnlockHandler(this);
        } else if (request.getMethod() == HttpMethod.OPTIONS) {
            handler = new OptionsHandler(this);
        } else if (request.getMethod() == HttpMethod.GET || request.getMethod() == HttpMethod.HEAD) {
            handler = new GetHandler(this);
        } else if (request.getMethod().name().equals("PROPFIND")) {
            handler = new PropfindHandler(this);
        } else if (request.getMethod().name().equals("MKCOL")) {
            throw new UnsupportedOperationException();
        } else if (request.getMethod() == HttpMethod.PUT) {
            handler = new PutHandler(this);
        } else if (request.getMethod() == HttpMethod.DELETE) {
            throw new UnsupportedOperationException();
        } else if (request.getMethod().name().equals("PROPPATCH")) {
            throw new UnsupportedOperationException();
        } else {
            log.warn("Method not allowed: " + request.getMethod());

            throw new WebdavResponseException(HttpResponseStatus.METHOD_NOT_ALLOWED);
        }

        return execute(handler);
    }

    private ListenableFuture<HttpObject> execute(MethodHandler handler) {
        ListenableFuture<HttpObject> future = Futures.dereference(getExecutor().submit(handler));

        return future;
    }

    private void setDavHeaders(HttpResponse response) {
        response.headers().set("DAV", "1,2");
        response.headers().set("MS-Author-Via", "DAV");
    }

    private static void setDateHeader(HttpResponse response) {
        Date now = new Date();
        HttpHeaders.setDate(response, now);
        // response.headers().set(HttpHeaders.Names.DATE, formatDate(time.getTime()));
    }

    @Override
    public String toString() {
        return "WebdavRequestHandler [request=" + request + "]";
    }

    public static ChunkAccumulator buildChunkAccumulator(HttpRequest httpRequest) {
        return new SimpleChunkAccumulator();
    }

    public static boolean isFatal(Throwable e) {
        boolean fatal = true;

        if (e instanceof WebdavResponseException) {
            fatal = ((WebdavResponseException) e).isFatal();
        } else {
            HttpResponseStatus status = mapToHttpStatus(e);
            fatal = isFatal(status);
        }

        return fatal;
    }

    private static boolean isFatal(HttpResponseStatus status) {
        return status.code() >= 500;
    }

    public FullHttpResponse buildError(Throwable e) {
        boolean fatal = isFatal(e);

        if (fatal) {
            log.warn("Error handling request", e);
        }

        HttpResponseStatus status = mapToHttpStatus(e);

        return buildFullResponse(status, !fatal);
    }

    static HttpResponseStatus mapToHttpStatus(Throwable e) {
        HttpResponseStatus status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        if (e instanceof WebdavResponseException) {
            WebdavResponseException w = (WebdavResponseException) e;
            status = w.getStatus();
        } else if (e instanceof FsFileAlreadyExistsException) {
            status = HttpResponseStatus.CONFLICT;
        } else if (e instanceof IOException) {
            if (e instanceof FileNotFoundException) {
                status = HttpResponseStatus.NOT_FOUND;
            }
        } else if (e instanceof IllegalArgumentException) {
            status = HttpResponseStatus.BAD_REQUEST;
        } else if (e instanceof SecurityException) {
            status = HttpResponseStatus.UNAUTHORIZED;
        }
        return status;
    }

    FullHttpResponse buildFullResponse(HttpResponseStatus status, boolean keepAlive) {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status);

        setHeaders(response, keepAlive);

        HttpHeaders.setContentLength(response, 0);

        return response;
    }

    public FullHttpResponse buildFullResponse(HttpResponseStatus status, boolean keepAlive, ByteBuf content) {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status, content);

        setHeaders(response, keepAlive);

        HttpHeaders.setContentLength(response, content.readableBytes());

        return response;
    }

    HttpResponse buildResponseHeader(HttpResponseStatus status, boolean keepAlive) {
        DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);

        setHeaders(response, keepAlive);

        return response;
    }

    private void setHeaders(HttpResponse response, boolean keepAlive) {
        if (!request.isKeepAlive()) {
            keepAlive = false;
        }

        HttpHeaders.setKeepAlive(response, keepAlive);

        // if (keepAlive) {
        // response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        // }

        setDateHeader(response);
        setDavHeaders(response);

    }

    public ListeningExecutorService getExecutor() {
        return webdav.syncActions.getExecutor();
    }

}
