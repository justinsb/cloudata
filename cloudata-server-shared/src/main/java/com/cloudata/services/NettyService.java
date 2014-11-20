package com.cloudata.services;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;

import com.google.common.util.concurrent.AbstractService;

public abstract class NettyService extends AbstractService {

    private NioEventLoopGroup group;

    private Channel serverChannel;

    @Override
    protected void doStart() {
        try {
            int nThreads = 1;
            group = new NioEventLoopGroup(nThreads, new DefaultThreadFactory("pool-redis"));

            ServerBootstrap b = buildBootstrap(group);

            // Start the server.
            Channel serverChannel = b.bind().sync().channel();

            this.serverChannel = serverChannel;

            // return f;
            this.notifyStarted();
        } catch (Exception e) {
            this.notifyFailed(e);
        }
    }

    protected abstract ServerBootstrap buildBootstrap(EventLoopGroup group);

    @Override
    protected void doStop() {
        try {
            ChannelFuture f2 = serverChannel.close();
            f2.sync();
        
            Future<?> shutdownFuture = group.shutdownGracefully();
            shutdownFuture.await();

            this.notifyStopped();
        } catch (Exception e) {
            this.notifyFailed(e);
        }

    }

}
