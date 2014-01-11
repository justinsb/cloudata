package com.cloudata.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.google.common.io.ByteSource;

public class ByteBufferByteSource extends ByteSource {

    final ByteBuffer buffer;

    public ByteBufferByteSource(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public InputStream openStream() throws IOException {
        return new ByteBufferInputStream(buffer.duplicate());
    }

    @Override
    public long size() throws IOException {
        return buffer.remaining();
    }

}
