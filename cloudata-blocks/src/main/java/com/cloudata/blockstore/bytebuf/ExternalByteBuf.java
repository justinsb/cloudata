package com.cloudata.blockstore.bytebuf;

import io.netty.buffer.ByteBuf;

import com.google.common.base.Throwables;

public class ExternalByteBuf extends MyDuplicatedByteBuf {

    final AutoCloseable external;

    public ExternalByteBuf(ByteBuf buffer, AutoCloseable external) {
        super(buffer);
        this.external = external;
    }

    @Override
    public final boolean release() {
        boolean retval = super.release();
        if (retval) {
            cleanup();
        }
        return retval;
    }

    @Override
    public final boolean release(int decrement) {
        boolean retval = super.release(decrement);
        if (retval) {
            cleanup();
        }
        return retval;
    }

    private void cleanup() {
        try {
            external.close();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

}
