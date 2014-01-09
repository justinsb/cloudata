package com.cloudata.btree.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.util.concurrent.ListenableFuture;

public interface BackingFile extends Closeable {

    void sync() throws IOException;

    ListenableFuture<Void> write(ByteBuffer src, final long position);

    // ListenableFuture<Void> write(final ByteBuf buf, final long position);

    long size();

    ListenableFuture<ByteBuffer> read(ByteBuffer buffer, long position);

    int getBlockSize();
}
