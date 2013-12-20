package com.cloudata.files.fs;

import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;

public class ByteStrings {

    public static ByteString encode(long v) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(v);
        buffer.flip();
        return ByteString.copyFrom(buffer);
    }

    public static long decodeLong(ByteString value) {
        if (value.size() != 8) {
            throw new IllegalArgumentException();
        }
        return value.asReadOnlyByteBuffer().getLong();
    }

}
