package com.cloudata.files.fs;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

public class EmptyChunkedInput<T> implements ChunkedInput<ByteBuf> {

    @Override
    public boolean isEndOfInput() throws Exception {
        return true;
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
        return null;
    }

}
