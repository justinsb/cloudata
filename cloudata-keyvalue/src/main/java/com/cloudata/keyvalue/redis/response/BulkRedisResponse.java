package com.cloudata.keyvalue.redis.response;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.redis.Codec;
import com.google.common.base.Charsets;

public class BulkRedisResponse extends RedisResponse {
    private static final Logger log = LoggerFactory.getLogger(BulkRedisResponse.class);

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

        log.debug("Built response: {}", this);
        log.debug("Built response length={}", length);
    }

    public BulkRedisResponse(ByteBuf bytes) {
        this.data = bytes;
        this.length = bytes.readableBytes();
    }

    public BulkRedisResponse(ByteBuffer data) {
        this(Unpooled.wrappedBuffer(data));
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
        log.debug("Writing message: {}", this);

        os.writeByte(MARKER);
        Codec.writeLong(os, length);
        os.writeBytes(CRLF);

        if (length >= 0) {
            assert data.readableBytes() == length;
            os.writeBytes(data.duplicate());
            // Special case: null response does not have a CRLF after the data
            os.writeBytes(CRLF);
        }
    }

    @Override
    public void encodeInline(ByteBuf os) {
        if (length > 0) {
            os.writeByte('+');
            os.writeBytes(data.duplicate());
        }
        os.writeBytes(CRLF);
    }

    @Override
    public String toString() {
        return asUTF8String();
    }
}