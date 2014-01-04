package com.cloudata.blockstore.bytebuf;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.io.ByteSource;

public class ByteBufByteSource extends ByteSource implements Closeable {

    private final ByteBuf buf;

    boolean closed;

    public ByteBufByteSource(ByteBuf buf) {
        this.buf = buf.duplicate();
        this.buf.retain();
    }

    @Override
    public long size() throws IOException {
        return buf.readableBytes();
    }

    @Override
    public InputStream openStream() throws IOException {
        return new ByteBufInputStream(buf.duplicate());
    }

    @Override
    public void close() throws IOException {
        assert !closed;
        closed = true;

        buf.release();
    }

}
