package com.cloudata.files.webdav.netty;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.nio.channels.ClosedChannelException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.files.webdav.WebdavContext;
import com.cloudata.files.webdav.WebdavRequest;
import com.cloudata.files.webdav.WebdavRequestHandler;
import com.cloudata.files.webdav.chunks.ChunkAccumulator;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class WebdavChannelHandler extends SimpleChannelInboundHandler<HttpObject> {
    private static final Logger log = LoggerFactory.getLogger(WebdavChannelHandler.class);

    final WebdavContext webdav;

    WebdavChannelHandler(WebdavContext webdav) {
        Preconditions.checkArgument(webdav != null);

        this.webdav = webdav;
    }

    WebdavRequest webdavRequest;
    WebdavRequestHandler handler;

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        // log.debug("Got message of type: {} {}", msg.getClass().getSimpleName(), msg);

        if (!msg.getDecoderResult().isSuccess()) {
            sendError(ctx, BAD_REQUEST);
            return;
        }

        boolean contentDone = false;

        try {
            if (msg instanceof HttpRequest) {
                HttpRequest httpRequest = (HttpRequest) msg;

                if (webdavRequest != null) {
                    throw new IllegalStateException();
                }

                ChunkAccumulator chunkAccumulator = WebdavRequestHandler.buildChunkAccumulator(httpRequest);

                webdavRequest = new WebdavRequest(httpRequest, chunkAccumulator);

                handler = new WebdavRequestHandler(webdav, ctx, webdavRequest);
            }

            if (msg instanceof HttpContent) {
                HttpContent httpContent = (HttpContent) msg;

                Preconditions.checkState(webdavRequest != null);

                webdavRequest.addChunk(httpContent.content());

            }

            if (msg instanceof LastHttpContent) {
                // Merge trailing headers into the message.
                LastHttpContent lastHttpContent = (LastHttpContent) msg;
                webdavRequest.addTrailingHeaders(lastHttpContent);

                contentDone = true;
            }
        } catch (Exception e) {
            log.error("Error during processing of message", e);
            ctx.close();
            return;
        }

        if (contentDone) {
            webdavRequest.endContent();

            processRequest(ctx, handler);

            webdavRequest = null;
            handler = null;
        }
    }

    private void processRequest(final ChannelHandlerContext ctx, final WebdavRequestHandler handler) {
        ListenableFuture<HttpObject> future;

        try {
            future = handler.processRequest();
        } catch (Exception e) {
            future = Futures.immediateFailedFuture(e);
        }

        Futures.addCallback(future, new FutureCallback<HttpObject>() {
            @Override
            public void onSuccess(HttpObject result) {
                if (result != null) {
                    sendResponse(result, true);
                } else {
                    // How can we wait until everything is written?
                    throw new IllegalStateException();
                    // handler.close();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                boolean isFatal = WebdavRequestHandler.isFatal(t);
                FullHttpResponse response = handler.buildError(t);
                log.info("Replying with error: " + response);

                sendResponse(response, !isFatal);
            }

            void sendResponse(HttpObject response, final boolean keepAlive) {
                if (!keepAlive) {
                    log.warn("Sending response without keepalive");
                }

                final ChannelFuture writeFuture = ctx.write(response);

                ctx.flush();

                writeFuture.addListener(new GenericFutureListener<Future<? super Void>>() {

                    @Override
                    public void operationComplete(Future<? super Void> future) throws Exception {
                        if (!keepAlive) {
                            writeFuture.channel().close();
                        }

                        try {
                            handler.close();
                        } catch (Exception e) {
                            log.warn("Error closing request object", e);
                        }
                    }

                });
            }

        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof ClosedChannelException) {
            assert !ctx.channel().isActive();
            log.info("Connection closed unexpectedly");
            return;
        }

        log.warn("exceptionCaught on webdav channel handler", cause);

        if (cause instanceof TooLongFrameException) {
            sendError(ctx, BAD_REQUEST, null);
            return;
        }

        if (ctx.channel().isActive()) {
            sendError(ctx, INTERNAL_SERVER_ERROR);
        }
    }

    private static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
        sendError(ctx, status, null);
    }

    public static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String message) {
        String error = "Failure: " + status.toString() + "\r\n";
        if (message != null) {
            error += message + "\r\n";
        }

        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer(error,
                Charsets.UTF_8));
        response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");

        // Close the connection as soon as the error message is sent.
        ctx.write(response).addListener(ChannelFutureListener.CLOSE);

        ctx.flush();
    }

}