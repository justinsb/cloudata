package com.cloudata.keyvalue.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class RedisResponseEncoder extends MessageToByteEncoder<RedisResponse> {
    @Override
    public void encode(ChannelHandlerContext ctx, RedisResponse msg, ByteBuf out) throws Exception {
        msg.encode(out);
    }
}