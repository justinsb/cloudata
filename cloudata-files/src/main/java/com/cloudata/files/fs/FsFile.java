package com.cloudata.files.fs;

import io.netty.buffer.ByteBuf;
import io.netty.handler.stream.ChunkedInput;

import java.io.Closeable;
import java.io.IOException;

public interface FsFile extends Closeable {
    ChunkedInput<ByteBuf> open() throws IOException;

    ChunkedInput<ByteBuf> open(Long from, Long to) throws IOException;

    long getLength();
}
