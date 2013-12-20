package com.cloudata.files.io;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.google.common.collect.ImmutableList;

public class ByteBufferListInputStream extends InputStream {
    final ImmutableList<ByteBuf> buffers;
    int index;
    int offset;

    int markIndex = -1;
    int markOffset = -1;

    public ByteBufferListInputStream(List<ByteBuf> buffers) {
        this.buffers = ImmutableList.copyOf(buffers);

        this.index = 0;
        this.offset = 0;
    }

    final byte[] singleReadBuffer = new byte[1];

    @Override
    public int read() throws IOException {
        int n = read(singleReadBuffer, 0, 1);
        if (n == -1) {
            return -1;
        }
        if (n != 1) {
            throw new IllegalStateException();
        }
        return singleReadBuffer[0] & 0xff;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        while (true) {
            if (index >= buffers.size()) {
                return -1;
            }

            ByteBuf buf = buffers.get(index);
            int remaining = buf.readableBytes() - offset;
            if (remaining > 0) {
                int n = Math.min(remaining, len);
                buf.getBytes(buf.readerIndex() + offset, b, off, n);
                offset += n;
                return n;
            } else {
                index++;
                offset = 0;
            }
        }
    }

    @Override
    public synchronized void mark(int readlimit) {
        markIndex = index;
        markOffset = offset;
    }

    @Override
    public synchronized void reset() throws IOException {
        index = markIndex;
        offset = markOffset;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

}
