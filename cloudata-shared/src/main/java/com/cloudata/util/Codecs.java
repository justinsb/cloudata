package com.cloudata.util;

import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;

public class Codecs {
    public static ByteString encodeInt32(int v) {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(v);
        b.flip();
        return ByteString.copyFrom(b);
    }

    public static ByteString encodeInt64(long v) {
        ByteBuffer b = ByteBuffer.allocate(8);
        b.putLong(v);
        b.flip();
        return ByteString.copyFrom(b);
    }

    public static int decodeInt32(ByteString b) {
        if (b.size() != 4) {
            throw new IllegalArgumentException();
        }
        return b.asReadOnlyByteBuffer().getInt();
    }

    public static long decodeInt64(ByteString b) {
        if (b.size() != 8) {
            throw new IllegalArgumentException();
        }
        return b.asReadOnlyByteBuffer().getLong();
    }

    public static long decodeInt64(ByteBuffer b) {
        if (b.remaining() != 8) {
            throw new IllegalArgumentException();
        }
        return b.getLong(b.position());
    }
}
