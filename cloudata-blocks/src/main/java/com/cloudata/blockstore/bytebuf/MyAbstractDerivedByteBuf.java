package com.cloudata.blockstore.bytebuf;

import io.netty.buffer.AbstractByteBuf;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public abstract class MyAbstractDerivedByteBuf extends AbstractByteBuf {

    protected MyAbstractDerivedByteBuf(int maxCapacity) {
        super(maxCapacity);
    }

    @Override
    public final int refCnt() {
        return unwrap().refCnt();
    }

    @Override
    public final ByteBuf retain() {
        unwrap().retain();
        return this;
    }

    @Override
    public final ByteBuf retain(int increment) {
        unwrap().retain(increment);
        return this;
    }

    @Override
    public/* final */boolean release() {
        return unwrap().release();
    }

    @Override
    public/* final */boolean release(int decrement) {
        return unwrap().release(decrement);
    }

    @Override
    public ByteBuffer internalNioBuffer(int index, int length) {
        return nioBuffer(index, length);
    }

    @Override
    public ByteBuffer nioBuffer(int index, int length) {
        return unwrap().nioBuffer(index, length);
    }
}
