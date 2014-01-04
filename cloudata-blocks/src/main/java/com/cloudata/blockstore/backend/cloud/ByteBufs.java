package com.cloudata.blockstore.backend.cloud;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;

public class ByteBufs {

    public static ByteBuf combine(List<ByteBuf> input) {
        if (input.size() == 1) {
            return input.get(0);
        }

        CompositeByteBuf accumulator = Unpooled.compositeBuffer(input.size());
        accumulator.addComponents(input);

        int size = 0;
        for (ByteBuf b : input) {
            size += b.readableBytes();
        }

        accumulator.writerIndex(accumulator.writerIndex() + size);

        return accumulator;
    }

}
