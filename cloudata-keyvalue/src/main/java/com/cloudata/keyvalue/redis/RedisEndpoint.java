package com.cloudata.keyvalue.redis;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.SocketAddress;

public class RedisEndpoint {
    NioEventLoopGroup group;

    final SocketAddress localAddress;

    final RedisServer redisServer;

    private Channel serverChannel;

    public RedisEndpoint(SocketAddress localAddress, RedisServer redisServer) {
        this.localAddress = localAddress;
        this.redisServer = redisServer;
    }

    public void start() throws InterruptedException {
        final RedisRequestHandler commandHandler = new RedisRequestHandler(redisServer);

        ServerBootstrap b = new ServerBootstrap();
        int nThreads = 1;
        group = new NioEventLoopGroup(nThreads, new DefaultThreadFactory("pool-redis"));

        b.group(group, group)

        .channel(NioServerSocketChannel.class)

        .option(ChannelOption.SO_BACKLOG, 100)

        .localAddress(localAddress)

        .childOption(ChannelOption.TCP_NODELAY, true)

        .childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                // p.addLast(new LoggingHandler(LogLevel.DEBUG));
                p.addLast(new RedisRequestDecoder());
                p.addLast(new RedisResponseEncoder());
                p.addLast(group, commandHandler);
            }

        });

        // Start the server.
        Channel serverChannel = b.bind().sync().channel();

        this.serverChannel = serverChannel;

        // return f;
    }

    public void stop() throws InterruptedException {
        // Future<?> f1 = group.shutdownGracefully();

        ChannelFuture f2 = serverChannel.close();
        group.shutdown();

        // f1.sync();
        f2.sync();
    }
}
