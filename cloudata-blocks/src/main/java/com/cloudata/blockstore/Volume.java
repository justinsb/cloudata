package com.cloudata.blockstore;

import io.netty.buffer.ByteBuf;

import com.google.common.util.concurrent.ListenableFuture;

public interface Volume {
    ListenableFuture<ByteBuf> read(long offset, long length);

    ListenableFuture<Void> write(long offset, long length, ByteBuf buf);

    ListenableFuture<Void> sync();

    int getChunkSize();
}
