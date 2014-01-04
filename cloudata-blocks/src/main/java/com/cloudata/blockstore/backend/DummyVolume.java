package com.cloudata.blockstore.backend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.blockstore.Volume;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class DummyVolume implements Volume {

    private static final Logger log = LoggerFactory.getLogger(DummyVolume.class);
    private static final int CHUNK_SIZE = 256 * 1024;

    private final long lun;

    public DummyVolume(long lun) {
        this.lun = lun;
    }

    @Override
    public ListenableFuture<ByteBuf> read(long offset, long length) {
        log.warn("DUMMY: read {} {}", offset, length);
        ByteBuf buf = Unpooled.buffer(Ints.checkedCast(length));
        return Futures.immediateFuture(buf);
    }

    @Override
    public void write(long offset, long length, ByteBuf buf) {
        Preconditions.checkState(length == buf.readableBytes());
        log.warn("DUMMY: write {} {}", offset, length);
    }

    @Override
    public ListenableFuture<Void> sync() {
        log.warn("DUMMY: sync");
        return Futures.immediateFuture(null);
    }

    @Override
    public int getChunkSize() {
        return CHUNK_SIZE;
    }

    @Override
    public long getLength() {
        return CHUNK_SIZE * 64;
    }

}
