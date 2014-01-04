package com.cloudata.blockstore.backend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.blockstore.Volume;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public class MmapVolume implements Volume {

    private static final Logger log = LoggerFactory.getLogger(MmapVolume.class);
    private static final int CHUNK_SIZE = 256 * 1024;

    private final File file;
    private final MappedByteBuffer mmap;
    private final long size;
    private final ListeningExecutorService executor;

    public MmapVolume(File file, ListeningExecutorService executor) throws IOException {
        this.file = file;
        this.executor = executor;

        long size = file.length();
        if (size == 0) {
            throw new IOException();
        }

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            this.mmap = raf.getChannel().map(MapMode.READ_WRITE, 0, size);
        }

        this.size = size;
    }

    @Override
    public ListenableFuture<ByteBuf> read(long offset, long length) {
        log.warn("MMAP: read {} {}", offset, length);

        ByteBuffer buffer = mmap.duplicate();
        buffer.position(Ints.checkedCast(offset));
        buffer.limit(Ints.checkedCast(offset + length));

        ByteBuf buf = Unpooled.wrappedBuffer(buffer);
        return Futures.immediateFuture(buf);
    }

    @Override
    public void write(long offset, long length, ByteBuf buf) {
        Preconditions.checkState(length == buf.readableBytes());

        log.warn("MMAP: write {} {}", offset, length);

        ByteBuffer buffer = mmap.duplicate();
        buffer.position(Ints.checkedCast(offset));
        buffer.limit(Ints.checkedCast(offset + length));

        buf.duplicate().readBytes(buffer);
    }

    @Override
    public ListenableFuture<Void> sync() {
        log.warn("MMAP: sync");

        return executor.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                mmap.force();

                return null;
            }

        });
    }

    @Override
    public int getChunkSize() {
        return CHUNK_SIZE;
    }

    @Override
    public long getLength() {
        return size;
    }

}
