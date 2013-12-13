package com.cloudata.keyvalue.redis;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.net.SocketAddress;

;

public class RedisEndpoint {
    DefaultEventExecutorGroup group;

    final SocketAddress localAddress;

    final RedisServer redisServer;

    public RedisEndpoint(SocketAddress localAddress, RedisServer redisServer) {
        this.localAddress = localAddress;
        this.redisServer = redisServer;
    }

    public void start() throws InterruptedException {
        final RedisRequestHandler commandHandler = new RedisRequestHandler(redisServer);

        ServerBootstrap b = new ServerBootstrap();
        int nThreads = 1;
        group = new DefaultEventExecutorGroup(nThreads);

        b.group(new NioEventLoopGroup(), new NioEventLoopGroup())

        .channel(NioServerSocketChannel.class)

        .option(ChannelOption.SO_BACKLOG, 100)

        .localAddress(localAddress)

        .childOption(ChannelOption.TCP_NODELAY, true)

        .childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                // p.addLast(new ByteLoggingHandler(LogLevel.INFO));
                p.addLast(new RedisRequestDecoder());
                p.addLast(new RedisResponseEncoder());
                p.addLast(group, commandHandler);
            }

        });

        // Start the server.
        ChannelFuture f = b.bind().sync();

        // Wait until the server socket is closed.
        f.channel().closeFuture().sync();

    }

    public void stop() {
        group.shutdownGracefully();
    }
}
