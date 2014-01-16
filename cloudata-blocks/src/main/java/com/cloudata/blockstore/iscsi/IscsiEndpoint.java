package com.cloudata.blockstore.iscsi;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;

import java.net.SocketAddress;

public class IscsiEndpoint {
    DefaultEventExecutorGroup group;

    final SocketAddress localAddress;

    final IscsiServer iscsiServer;

    private Channel serverChannel;

    IscsiEndpoint(SocketAddress localAddress, IscsiServer iscsiServer) {
        this.localAddress = localAddress;
        this.iscsiServer = iscsiServer;
    }

    public void start() throws InterruptedException {
        final IscsiRequestHandler commandHandler = new IscsiRequestHandler();

        ServerBootstrap b = new ServerBootstrap();
        int nThreads = 1;
        group = new DefaultEventExecutorGroup(nThreads, new DefaultThreadFactory("pool-iscsi"));

        b.group(new NioEventLoopGroup(), new NioEventLoopGroup())

        .channel(NioServerSocketChannel.class)

        .option(ChannelOption.SO_BACKLOG, 100)

        .localAddress(localAddress)

        .childOption(ChannelOption.TCP_NODELAY, true)

        .childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                // p.addLast(new LoggingHandler(LogLevel.DEBUG));
                p.addLast(new IscsiResponseEncoder());
                p.addLast(new IscsiRequestDecoder(iscsiServer));
                p.addLast(group, commandHandler);
            }

        });

        // Start the server.
        Channel serverChannel = b.bind().sync().channel();

        this.serverChannel = serverChannel;

        // return f;
    }

    public void stop() throws InterruptedException {
        Future<?> f1 = group.shutdownGracefully();

        ChannelFuture f2 = serverChannel.close();

        f1.sync();
        f2.sync();
    }
}
