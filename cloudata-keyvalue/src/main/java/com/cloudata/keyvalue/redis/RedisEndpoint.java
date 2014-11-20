package com.cloudata.keyvalue.redis;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

import com.cloudata.services.NettyService;
import com.google.common.net.HostAndPort;

public class RedisEndpoint extends NettyService {

    final HostAndPort hostAndPort;

    final RedisServer redisServer;

    public RedisEndpoint(HostAndPort hostAndPort, RedisServer redisServer) {
        this.hostAndPort = hostAndPort;
        this.redisServer = redisServer;
    }

    @Override
    protected ServerBootstrap buildBootstrap(final EventLoopGroup group) {
        final RedisRequestHandler commandHandler = new RedisRequestHandler(redisServer);

        ServerBootstrap serverBootstrap = new ServerBootstrap();

        serverBootstrap.group(group, group)

        .channel(NioServerSocketChannel.class)

        .option(ChannelOption.SO_BACKLOG, 100)

        .localAddress(new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort()))

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

        return serverBootstrap;
    }
}
