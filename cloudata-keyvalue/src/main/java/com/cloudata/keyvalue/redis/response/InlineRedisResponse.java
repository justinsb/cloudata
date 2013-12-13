package com.cloudata.keyvalue.redis.response;

import io.netty.buffer.ByteBuf;

public class InlineRedisResponse extends RedisResponse {
    final RedisResponse inner;

    public InlineRedisResponse(RedisResponse inner) {
        this.inner = inner;
    }

    @Override
    public void encode(ByteBuf out) {
        if (inner == null) {
            out.writeBytes(CRLF);
        } else {
            inner.encodeInline(out);
        }
    }

    @Override
    public void encodeInline(ByteBuf out) {
        throw new IllegalStateException();
    }

}
