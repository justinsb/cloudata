package com.cloudata.keyvalue.redis;

import io.netty.buffer.ByteBuf;

import com.google.common.base.Charsets;

public class ErrorRedisReponse extends RedisResponse {
    public static final byte MARKER = '-';

    public static final ErrorRedisReponse NOT_IMPLEMENTED = new ErrorRedisReponse("Not yet implemented");

    private final String message;
    private final byte[] messageBytes;

    public ErrorRedisReponse(String message) {
        this.message = message;
        this.messageBytes = message.getBytes(Charsets.UTF_8);
    }

    // @Override
    // public String data() {
    // return message;
    // }

    @Override
    public void encode(ByteBuf os) {
        os.writeByte(MARKER);
        os.writeBytes(messageBytes);
        os.writeBytes(CRLF);
    }

    @Override
    public void encodeInline(ByteBuf os) {
        os.writeByte('-');
        os.writeBytes(messageBytes);
        os.writeBytes(CRLF);
    }

    @Override
    public String toString() {
        return "ERROR:" + message;
    }

}
