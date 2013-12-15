package com.cloudata.git.jgit;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.eclipse.jgit.internal.storage.dfs.ReadableChannel;

import com.cloudata.objectstore.ObjectStorePath;

public class BlobReadableChannel implements ReadableChannel {

    // ReadableChannel recommends against >4MB
    static final int FETCH_CHUNK_SIZE = 4 * 1024 * 1024;

    final ObjectStorePath path;
    final long length;

    private long position;

    private boolean open = true;

    public BlobReadableChannel(ObjectStorePath path, long length) {
        this.path = path;
        this.length = length;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (length == position) {
            return -1;
        }

        long n = path.read(dst, position);
        position += n;
        return (int) n;
    }

    @Override
    public void close() {
        open = false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public void position(long newPosition) {
        position = newPosition;
    }

    @Override
    public long size() {
        return length;
    }

    @Override
    public int blockSize() {
        return FETCH_CHUNK_SIZE;
    }

}
