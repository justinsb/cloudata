package com.cloudata.files.webdav.chunks;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.io.InputStream;

import com.google.common.io.ByteSource;

public class ByteBufByteSource extends ByteSource {
    final ByteBuf buf;

    public ByteBufByteSource(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public InputStream openStream() throws IOException {
        return new ByteBufInputStream(buf.duplicate());
    }

}
