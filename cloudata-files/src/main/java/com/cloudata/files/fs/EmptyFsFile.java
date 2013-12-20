package com.cloudata.files.fs;

import io.netty.buffer.ByteBuf;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;

public class EmptyFsFile implements FsFile {

    @Override
    public void close() throws IOException {

    }

    @Override
    public ChunkedInput<ByteBuf> open() throws IOException {
        return new EmptyChunkedInput<ByteBuf>();
    }

    @Override
    public ChunkedInput<ByteBuf> open(Long from, Long to) throws IOException {
        return new EmptyChunkedInput<ByteBuf>();
    }

    @Override
    public long getLength() {
        return 0;
    }

}
