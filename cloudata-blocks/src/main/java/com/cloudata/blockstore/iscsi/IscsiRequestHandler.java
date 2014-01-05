package com.cloudata.blockstore.iscsi;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Handle decoded commands
 */
@ChannelHandler.Sharable
public class IscsiRequestHandler extends SimpleChannelInboundHandler<IscsiRequest> {
    private static final Logger log = LoggerFactory.getLogger(IscsiRequestHandler.class);

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, final IscsiRequest msg) throws Exception {
        ListenableFuture<Void> future = null;
        try {
            log.debug("Executing command: {}", msg);

            future = msg.start();
            msg.retain();
        } catch (Exception e) {
            log.warn("Error starting command: " + msg, e);
            throw e;
        }

        Futures.addCallback(future, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                log.debug("Finished processing command: " + msg);
                msg.release();
            }

            @Override
            public void onFailure(Throwable t) {
                log.warn("Error processing command: " + msg, t);
                msg.release();
            }
        });
    }
}