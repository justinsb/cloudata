package com.cloudata.keyvalue.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;

import com.google.common.base.Charsets;

public class BulkRedisResponse extends RedisResponse {
    public static final BulkRedisResponse NIL_REPLY = new BulkRedisResponse();

    public static final char MARKER = '$';
    private final ByteBuf data;
    private final int length;

    private BulkRedisResponse() {
        this.data = null;
        this.length = -1;
    }

    public BulkRedisResponse(byte[] bytes) {
        this.data = Unpooled.wrappedBuffer(bytes);
        this.length = bytes.length;
    }

    public BulkRedisResponse(ByteBuf bytes) {
        this.data = bytes;
        this.length = bytes.capacity();
    }

    // @Override
    // public ByteBuf data() {
    // return data;
    // }

    public String asAsciiString() {
        if (data == null) {
            return null;
        }
        return data.toString(Charsets.US_ASCII);
    }

    public String asUTF8String() {
        if (data == null) {
            return null;
        }
        return data.toString(Charsets.UTF_8);
    }

    public String asString(Charset charset) {
        if (data == null) {
            return null;
        }
        return data.toString(charset);
    }

    @Override
    public void encode(ByteBuf os) {
        os.writeByte(MARKER);
        Codec.writeLong(os, length);
        if (length > 0) {
            os.writeBytes(data);

            // Special case: null response does not have a CRLF
            os.writeBytes(CRLF);
        }
    }

    @Override
    public void encodeInline(ByteBuf os) {
        if (length > 0) {
            os.writeByte('+');
            os.writeBytes(data);
        }
        os.writeBytes(CRLF);
    }

    @Override
    public String toString() {
        return asUTF8String();
    }
}