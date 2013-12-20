package com.cloudata.files.webdav;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.Callable;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.files.fs.FsClient;
import com.cloudata.files.fs.FsCredentials;
import com.cloudata.files.fs.FsPath;
import com.cloudata.files.fs.FsVolume;
import com.cloudata.files.locks.LockService;
import com.cloudata.files.webdav.chunks.ChunkAccumulator;
import com.cloudata.files.webdav.model.WebdavXmlWriter;
import com.cloudata.files.webdav.model.XmlSerializable;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public abstract class MethodHandler implements Callable<ListenableFuture<HttpObject>>, Closeable {

    private static final Logger log = LoggerFactory.getLogger(MethodHandler.class);

    final WebdavRequestHandler requestHandler;

    public MethodHandler(WebdavRequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    @Override
    public void close() {
    }

    @Override
    public final ListenableFuture<HttpObject> call() throws Exception {
        WebdavRequest request = getRequest();

        final FsCredentials credentials = request.getCredentials();
        if (credentials == null) {
            FullHttpResponse response = requestHandler.buildFullResponse(HttpResponseStatus.UNAUTHORIZED, true);
            response.headers().set("WWW-Authenticate", "Basic realm=\"webdav\"");
            return Futures.<HttpObject> immediateFuture(response);
        }

        String ifHeader = request.getHeader("If");
        if (!Strings.isNullOrEmpty(ifHeader)) {
            if (!checkIfHeader(ifHeader)) {
                log.info("If precondition was false: {}", ifHeader);
                throw new WebdavResponseException(HttpResponseStatus.PRECONDITION_FAILED);
            }
        }
        ListenableFuture<FsPath> fsPathFuture = resolveFsPath();

        return Futures.transform(fsPathFuture, new AsyncFunction<FsPath, HttpObject>() {
            @Override
            public ListenableFuture<HttpObject> apply(FsPath fsPath) throws Exception {

                if (fsPath == null && requiresValidPath()) {
                    throw new FileNotFoundException();
                }

                return doActionAsync(fsPath);
            }
        });
    }

    protected boolean requiresValidPath() {
        // TODO: I think locking doesn't require a valid path
        return true;
    }

    protected abstract HttpObject doAction(FsPath fsPath) throws Exception;

    protected LockService getLockService() {
        return this.requestHandler.webdav.lockService;
    }

    protected ListenableFuture<HttpObject> doActionAsync(FsPath fsPath) throws Exception {
        return Futures.immediateFuture(doAction(fsPath));
    }

    private boolean checkIfHeader(String ifHeader) {
        // See: http://www.webdav.org/specs/rfc2518.html#HEADER_If
        String subjectUrl = null;

        for (String token : Splitter.on(" ").omitEmptyStrings().trimResults().split(ifHeader)) {
            if (token.startsWith("<")) {
                // URL
                if (!token.endsWith(">")) {
                    throw new IllegalArgumentException("Could not parse ifHeader: " + ifHeader);
                }

                subjectUrl = token.substring(1, token.length() - 1);
            } else if (token.startsWith("(")) {
                if (token.startsWith("(<")) {
                    if (!token.endsWith(">)")) {
                        throw new IllegalArgumentException("Could not parse ifHeader: " + ifHeader);
                    }

                    String lockToken = token.substring(2, token.length() - 2);

                    if (lockToken.startsWith(LockHandler.LOCK_PREFIX)) {
                        lockToken = lockToken.substring(LockHandler.LOCK_PREFIX.length());
                    }

                    String lockSubject;

                    if (Strings.isNullOrEmpty(subjectUrl)) {
                        lockSubject = buildLockSubject(getRequest());
                    } else {
                        lockSubject = buildLockSubject(subjectUrl);
                    }

                    if (getLockService().findLock(lockSubject, lockToken) == null) {
                        return false;
                    }
                } else {
                    throw new IllegalArgumentException("Could not parse ifHeader: " + ifHeader);
                }
            } else {
                throw new IllegalArgumentException("Could not parse ifHeader: " + ifHeader);
            }
        }
        // <https://127.0.0.1:8443/d1/d> (<opaquelocktoken:31dafad5-2cbb-49c0-946f-c3124aca0922>)
        return true;
    }

    protected HttpResponse buildResponseHeader(HttpResponseStatus status) {
        return requestHandler.buildResponseHeader(status, true);
    }

    protected FullHttpResponse buildFullResponse(HttpResponseStatus status) {
        return requestHandler.buildFullResponse(status, true);
    }

    protected ChunkAccumulator getContent() {
        return getRequest().getContent();
    }

    protected WebdavRequest getRequest() {
        return requestHandler.request;
    }

    protected String getUri() {
        return getRequest().getUri();
    }

    protected FsClient getFsClient() {
        return requestHandler.webdav.fsClient;
    }

    protected MimeHelper getMimeHelper() {
        return requestHandler.webdav.mimeHelper;
    }

    protected ChannelFuture write(Object msg) {
        return requestHandler.ctx.write(msg);
    }

    protected void setDateAndCacheHeaders(FsPath fsPath, HttpResponse response, int httpCacheSeconds) {
        // Last modified header
        response.headers().set(HttpHeaders.Names.LAST_MODIFIED, DateFormatter.get().format(fsPath.getModified()));

        // Date header
        Calendar now = new GregorianCalendar();
        HttpHeaders.setDate(response, now.getTime());

        // Calendar time = setDateHeader(response);

        // Add cache headers
        now.add(Calendar.SECOND, httpCacheSeconds);
        HttpHeaders.setDateHeader(response, HttpHeaders.Names.EXPIRES, now.getTime());
        response.headers().set(HttpHeaders.Names.CACHE_CONTROL, "private, max-age=" + httpCacheSeconds);
    }

    HttpResponse buildXmlResponse(HttpResponseStatus status, XmlSerializable payload) throws XMLStreamException {
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XMLStreamWriter xmlStreamWriter = xmlOutputFactory.createXMLStreamWriter(baos, "UTF-8");
        xmlStreamWriter.writeStartDocument();
        payload.write(new WebdavXmlWriter(xmlStreamWriter));
        byte[] xmlData = baos.toByteArray();

        if (log.isDebugEnabled()) {
            String xml = new String(xmlData, Charsets.UTF_8);
            log.debug("XML: " + xml);
        }

        ByteBuf content = Unpooled.copiedBuffer(xmlData);
        boolean keepAlive = true;
        FullHttpResponse response = requestHandler.buildFullResponse(status, keepAlive, content);
        response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/xml; charset=UTF-8");

        assert content.readableBytes() == xmlData.length;
        // HttpHeaders.setContentLength(response, xmlData.length);

        return response;
    }

    public ListenableFuture<FsPath> resolveFsPath() {
        WebdavRequest request = getRequest();

        log.info(request.getMethod() + " " + request.getUri());
        log.debug("Request: " + request);

        final FsCredentials credentials = request.getCredentials();
        if (credentials == null) {
            // TODO: Can we send the response immediately (to avoid a huge POST)?

            // Don't try to resolve for unauthenticated users...
            return Futures.immediateFuture(null);
        }

        String path = request.getUri();

        if (shouldResolveParent()) {
            path = Urls.getParentPath(path);
            if (Strings.isNullOrEmpty(path)) {
                return Futures.immediateFuture(null);
            }
        }

        final String resolvePath = path;

        return getExecutor().submit(new Callable<FsPath>() {
            @Override
            public FsPath call() throws Exception {
                String host = "";
                String path = resolvePath;

                List<String> pathTokens = Urls.pathToTokens(path);

                FsVolume volume = FsVolume.fromHost(host);
                return getFsClient().resolve(volume, credentials, pathTokens);
            }
        });
    }

    protected boolean shouldResolveParent() {
        return false;
    }

    private ListeningExecutorService getExecutor() {
        return requestHandler.getExecutor();
    }

    protected String buildLockSubject(WebdavRequest request) {
        String uri = request.getUri();
        return buildLockSubject(uri);
    }

    protected String buildLockSubject(String uriString) {
        URI uri = URI.create(uriString);
        return uri.getPath();
    }
}
