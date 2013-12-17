package com.cloudata.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
    ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() throws IOException {
        if (buffer.remaining() == 0) {
            return -1;
        }
        return buffer.get() & 0xff;
    }

    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int remaining = buffer.remaining();
        if (remaining == 0) {
            return -1;
        }

        if (len > remaining) {
            len = remaining;
        }

        buffer.get(b, off, len);
        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        int remaining = buffer.remaining();
        if (n > remaining) {
            n = remaining;
        }
        buffer.position(buffer.position() + (int) n);
        return n;
    }

}
