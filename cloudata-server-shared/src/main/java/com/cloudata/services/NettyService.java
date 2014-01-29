package com.cloudata.services;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import com.google.common.util.concurrent.AbstractService;

public abstract class NettyService extends AbstractService {

    NioEventLoopGroup group;

    private Channel serverChannel;

    @Override
    protected void doStart() {
        try {
            int nThreads = 1;
            group = new NioEventLoopGroup(nThreads, new DefaultThreadFactory("pool-redis"));

            ServerBootstrap b = buildBootstrap();

            // Start the server.
            Channel serverChannel = b.bind().sync().channel();

            this.serverChannel = serverChannel;

            // return f;
            this.notifyStarted();
        } catch (Exception e) {
            this.notifyFailed(e);
        }
    }

    protected abstract ServerBootstrap buildBootstrap();

    @Override
    protected void doStop() {
        try {
            ChannelFuture f2 = serverChannel.close();
            group.shutdown();

            // f1.sync();
            f2.sync();
            this.notifyStopped();
        } catch (Exception e) {
            this.notifyFailed(e);
        }

    }

}
