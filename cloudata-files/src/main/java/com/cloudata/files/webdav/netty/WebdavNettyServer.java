package com.cloudata.files.webdav.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebdavNettyServer {
    static final Logger log = LoggerFactory.getLogger(WebdavNettyServer.class);

    final WebdavChannelInitializer channelInitializer;

    @Inject
    public WebdavNettyServer(WebdavChannelInitializer channelInitializer) {
        this.channelInitializer = channelInitializer;
    }

    private Channel channel;

    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;

    public Channel start(int port) throws InterruptedException {
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(channelInitializer);

        this.channel = b.bind(port).sync().channel();

        log.info("Listening on port {}", port);
        return channel;
    }

    public void stop() throws InterruptedException {
        channel.close().sync();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    public boolean isStarted() {
        return channel != null;
    }
}
