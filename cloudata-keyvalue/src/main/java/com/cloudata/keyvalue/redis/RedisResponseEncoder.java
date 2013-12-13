package com.cloudata.keyvalue.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.redis.response.RedisResponse;

public class RedisResponseEncoder extends MessageToByteEncoder<RedisResponse> {
    private static final Logger log = LoggerFactory.getLogger(RedisResponseEncoder.class);

    @Override
    public void encode(ChannelHandlerContext ctx, RedisResponse msg, ByteBuf out) throws Exception {
        try {
            msg.encode(out);
        } catch (Throwable t) {
            log.warn("Error during encoding", t);
            throw t;
        }
    }
}