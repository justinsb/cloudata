package com.cloudata.keyvalue.redis;

import io.netty.buffer.ByteBuf;

public abstract class RedisResponse {
    public static final byte[] CRLF = new byte[] { '\r', '\n' };

    public abstract void encode(ByteBuf out);

    public abstract void encodeInline(ByteBuf out);
}
