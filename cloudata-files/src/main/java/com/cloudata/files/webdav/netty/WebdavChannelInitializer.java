package com.cloudata.files.webdav.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.cloudata.files.webdav.WebdavContext;

@Singleton
public class WebdavChannelInitializer extends ChannelInitializer<SocketChannel> {
    final WebdavContext webdav;

    @Inject
    public WebdavChannelInitializer(WebdavContext webdav) {
        this.webdav = webdav;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("decoder", new HttpRequestDecoder());
        // pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());

        pipeline.addLast("handler", new WebdavChannelHandler(webdav));
    }

}
