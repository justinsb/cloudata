package com.cloudata.btree.caching;

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.nio.ByteBuffer;

import com.cloudata.btree.io.BackingFile;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class CacheEntry implements Closeable {
    final int pageNumber;
    final int size;
    final ByteBuf buffer;

    public static enum State {
        READY_FOR_WRITE, READY_FOR_READ, HAVE_DATA
    }

    State state;
    ListenableFuture<ByteBuffer> contents;

    @Override
    public String toString() {
        return "CacheEntry [pageNumber=" + pageNumber + ", size=" + size + ", bufferRefCnt=" + buffer.refCnt() + "]";
    }

    public CacheEntry(State state, int pageNumber, int size, ByteBuf buffer) {
        this.state = state;
        this.pageNumber = pageNumber;
        this.size = size;
        this.buffer = buffer;

        assert buffer.capacity() == size;
        assert buffer.readerIndex() == 0;
        assert buffer.writerIndex() == 0;
        assert buffer.refCnt() == 1;
    }

    public int weight() {
        return size;
    }

    public int size() {
        return size;
    }

    @Override
    public void close() {
        buffer.release();
    }

    // This needs to be async because of joining behaviour
    // TODO: But maybe only internally
    public ListenableFuture<ByteBuffer> read(BackingFile backingFile, long offset) {
        synchronized (this) {
            if (state == State.HAVE_DATA) {
                return Futures.immediateFuture(buffer.nioBuffer());
            }
            Preconditions.checkState(state == State.READY_FOR_READ);

            if (contents != null) {
                return contents;
            }

            // Make sure buffer remains valid for duration of read
            buffer.retain();

            assert buffer.readerIndex() == 0;
            assert buffer.writerIndex() == 0;
            final ByteBuffer nioBuffer = buffer.nioBuffer(0, size);

            assert nioBuffer.position() == 0;
            assert nioBuffer.remaining() == size;

            // Note we use ByteBuf overload, which handles release on error
            ListenableFuture<ByteBuffer> future = backingFile.read(nioBuffer, offset);
            this.contents = future;

            // TODO: Would be nice to return a read-only buffer
            Futures.addCallback(future, new FutureCallback<ByteBuffer>() {

                @Override
                public void onSuccess(ByteBuffer result) {
                    synchronized (this) {
                        // Reduce memory retained
                        assert state == State.READY_FOR_READ;
                        state = State.HAVE_DATA;
                        contents = null;

                        // Buffer is returned pre-flipped
                        assert result.remaining() == size;

                        buffer.writerIndex(size);
                        assert buffer.readableBytes() == size;
                    }
                    buffer.release();
                }

                @Override
                public void onFailure(Throwable t) {
                    synchronized (this) {
                        // Allow for retry
                        assert state == State.READY_FOR_READ;
                        state = State.READY_FOR_READ;
                        contents = null;
                    }
                    buffer.release();
                }

            });
            return future;
        }
    }

    public void markWriteComplete() {
        synchronized (this) {
            // Allow for retry
            assert state == State.READY_FOR_WRITE;
            state = State.HAVE_DATA;
        }
    }

    public synchronized State getState() {
        return state;
    }

    public ByteBuf getBuffer() {
        return buffer;
    }
}
