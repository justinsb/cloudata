package com.cloudata.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.io.ByteSource;
import com.google.protobuf.ByteString;

public class ByteStrings {

    public static ByteSource asByteSource(ByteString s) {
        return new ByteStringByteSource(s);
    }

    public static class ByteStringByteSource extends ByteSource {
        final ByteString s;

        public ByteStringByteSource(ByteString s) {
            this.s = s;
        }

        @Override
        public InputStream openStream() throws IOException {
            return s.newInput();
        }

        @Override
        public InputStream openBufferedStream() throws IOException {
            return openStream();
        }

        @Override
        public long size() throws IOException {
            return s.size();
        }

        @Override
        public long copyTo(OutputStream output) throws IOException {
            s.writeTo(output);
            return s.size();
        }

    }

    public static HashCode hash(HashFunction hash, ByteString data) throws IOException {
        return asByteSource(data).hash(hash);
    }

    public static ByteString encode(int v) {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(v);
        b.flip();
        return ByteString.copyFrom(b);
    }

    public static ByteString encode(long v) {
        ByteBuffer b = ByteBuffer.allocate(8);
        b.putLong(v);
        b.flip();
        return ByteString.copyFrom(b);
    }
}
