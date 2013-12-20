package com.cloudata.files.fs;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

public class SliceByteInput implements ChunkedInput<ByteBuf> {
    private final ChunkedInput<ByteBuf> inner;

    long position = 0;

    private final Long from;
    private final Long to;

    public SliceByteInput(ChunkedInput<ByteBuf> inner, Long from, Long to) {
        this.inner = inner;
        this.from = from;
        this.to = to;
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        if (to != null) {
            if (position >= to) {
                return true;
            }
        }
        return inner.isEndOfInput();
    }

    @Override
    public void close() throws Exception {
        // log.info("Closed SliceByteInput");

        inner.close();
    }

    @Override
    public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
        while (true) {
            ByteBuf buffer = null;
            boolean release = true;

            try {
                buffer = inner.readChunk(ctx);

                if (buffer == null) {
                    return null;
                }

                // log.info("SliceByteInput position=" + position);

                if (from != null) {
                    if (position < from) {
                        int skip = (int) Math.min(from - position, buffer.readableBytes());
                        if (skip > 0) {
                            buffer.skipBytes(skip);
                            position += skip;
                        }
                    }
                }

                int consumed = buffer.readableBytes();

                if (to != null) {
                    long n = to - position;
                    long limitWriterIndex = buffer.readerIndex() + n;

                    if (limitWriterIndex < buffer.writerIndex()) {
                        buffer.writerIndex((int) limitWriterIndex);
                    }

                    if (position > to && !buffer.isReadable()) {
                        return null;
                    }
                }

                position += consumed;

                if (buffer.isReadable()) {
                    release = false;
                    return buffer;
                }
            } finally {
                if (release && buffer != null) {
                    buffer.release();
                }
            }
        }
    }
}
