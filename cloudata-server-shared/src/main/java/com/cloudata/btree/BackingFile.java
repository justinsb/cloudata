package com.cloudata.btree;

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class BackingFile implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(BackingFile.class);

    final File file;

    final AsynchronousFileChannel fileChannel;

    public BackingFile(File file) throws IOException {
        this.file = file;

        Path path = Paths.get(file.getAbsolutePath());

        log.warn("We want to disable the read cache (probably?)");
        this.fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ);
    }

    public ListenableFuture<ByteBuffer> read(ByteBuffer buffer, long position) {
        final SettableFuture<ByteBuffer> future = SettableFuture.create();

        read(buffer, buffer.position(), position, future);

        return future;
    }

    /**
     * Read into a ByteBuf. Adds reference for duration of read, and releases it unless future is set.
     */
    public ListenableFuture<ByteBuf> read(final ByteBuf buf, final long position) {
        final SettableFuture<ByteBuf> ret = SettableFuture.create();

        ByteBuffer nioBuffer = buf.nioBuffer();

        assert nioBuffer.remaining() == buf.writableBytes();

        ListenableFuture<ByteBuffer> future = read(buf.nioBuffer(), position);

        buf.retain();

        Futures.addCallback(future, new FutureCallback<ByteBuffer>() {
            @Override
            public void onSuccess(ByteBuffer result) {
                assert buf.readableBytes() == result.remaining();

                // Caller releases
                ret.set(buf);
            }

            @Override
            public void onFailure(Throwable t) {
                buf.release();

                ret.setException(t);
            }
        });

        return ret;
    }

    private void read(ByteBuffer dest, final int destStart, final long filePosition,
            final SettableFuture<ByteBuffer> future) {
        try {
            fileChannel.read(dest, filePosition, dest, new CompletionHandler<Integer, ByteBuffer>() {
                @Override
                public void completed(Integer n, ByteBuffer dest) {
                    if (n == -1) {
                        future.setException(new EOFException());
                        return;
                    }

                    if (dest.remaining() > 0) {
                        long newPosition = filePosition + n;

                        read(dest, destStart, newPosition, future);
                        return;
                    }

                    dest.position(destStart);
                    future.set(dest);
                }

                @Override
                public void failed(Throwable exc, ByteBuffer attachment) {
                    future.setException(exc);
                }
            });
        } catch (Exception e) {
            future.setException(e);
        }
    }

    public void sync() throws IOException {
        boolean metadata = false;
        fileChannel.force(metadata);
    }

    public ListenableFuture<Void> write(ByteBuffer src, final long position) {
        final SettableFuture<Void> future = SettableFuture.create();
        write(src, position, future);
        return future;
    }

    public ListenableFuture<Void> write(final ByteBuf buf, final long position) {
        ListenableFuture<Void> future = write(buf.nioBuffer(), position);

        buf.retain();

        Futures.addCallback(future, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                buf.release();
            }

            @Override
            public void onFailure(Throwable t) {
                buf.release();
            }
        });

        return future;
    }

    private void write(ByteBuffer src, final long position, final SettableFuture<Void> future) {
        try {
            fileChannel.write(src, position, src, new CompletionHandler<Integer, ByteBuffer>() {
                @Override
                public void completed(Integer wrote, ByteBuffer src) {
                    if (wrote < 0) {
                        future.setException(new IOException());
                        return;
                    }

                    if (src.remaining() > 0) {
                        long newPosition = position + wrote;

                        write(src, newPosition, future);
                        return;
                    }

                    future.set(null);
                }

                @Override
                public void failed(Throwable exc, ByteBuffer attachment) {
                    future.setException(exc);
                }
            });
        } catch (Exception e) {
            future.setException(e);
        }
    }

    public long size() {
        return file.length();
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }

}
