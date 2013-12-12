package com.cloudata.keyvalue.redis;

import io.netty.buffer.ByteBuf;

import com.google.common.base.Charsets;

public class StatusRedisResponse extends RedisResponse {
    public static final byte MARKER = '+';

    public static final StatusRedisResponse OK = new StatusRedisResponse("OK");
    public static final StatusRedisResponse QUIT = new StatusRedisResponse("QUIT");
    public static final StatusRedisResponse PONG = new StatusRedisResponse("PONG");

    private final String status;
    private final byte[] statusBytes;

    public StatusRedisResponse(String status) {
        this.status = status;
        this.statusBytes = status.getBytes(Charsets.UTF_8);
    }

    // @Override
    // public String data() {
    // return status;
    // }

    @Override
    public void encode(ByteBuf os) {
        os.writeByte(MARKER);
        os.writeBytes(statusBytes);
        os.writeBytes(CRLF);
    }

    @Override
    public void encodeInline(ByteBuf os) {
        encode(os);
    }

    @Override
    public String toString() {
        return "STATUS:" + status;
    }

}
