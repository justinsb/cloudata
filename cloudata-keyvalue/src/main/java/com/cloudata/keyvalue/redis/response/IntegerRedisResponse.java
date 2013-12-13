package com.cloudata.keyvalue.redis.response;

import com.cloudata.keyvalue.redis.Codec;

import io.netty.buffer.ByteBuf;

public class IntegerRedisResponse extends RedisResponse {
    public static final char MARKER = ':';

    private final long value;

    private static final int cacheLow = -255;
    private static final int cacheHigh = 255;
    private static IntegerRedisResponse[] cache;

    static {
        cache = new IntegerRedisResponse[1 + cacheHigh - cacheLow];
        for (int i = 0; i < cache.length; i++) {
            cache[i + cacheLow] = new IntegerRedisResponse(i);
        }
    }

    public static IntegerRedisResponse valueOf(long v) {
        if (v >= cacheLow && v <= cacheHigh) {
            return cache[(int) (v - cacheLow)];
        } else {
            return new IntegerRedisResponse(v);
        }
    }

    public IntegerRedisResponse(long value) {
        this.value = value;
    }

    // @Override
    // public Long data() {
    // return value;
    // }

    @Override
    public void encode(ByteBuf os) {
        os.writeByte(MARKER);
        Codec.writeLong(os, value);
        os.writeBytes(CRLF);
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }

    @Override
    public void encodeInline(ByteBuf out) {
        encode(out);
    }
}