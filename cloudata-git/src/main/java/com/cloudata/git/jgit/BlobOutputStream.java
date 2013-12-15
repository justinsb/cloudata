package com.cloudata.git.jgit;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream;

import com.cloudata.objectstore.ObjectStorePath;

public class BlobOutputStream extends DfsOutputStream {
    boolean closed;

    final File temp;
    final FileOutputStream out;
    final FileChannel in;

    final ObjectStorePath path;

    public BlobOutputStream(ObjectStorePath path, File temp) throws IOException {
        this.temp = temp;
        this.out = new FileOutputStream(temp);
        this.in = new RandomAccessFile(temp, "r").getChannel();
        this.path = path;

        this.closed = false;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
        out.write(buf, off, len);
    }

    @Override
    public int read(long position, ByteBuffer buf) throws IOException {
        in.position(position);
        int n = in.read(buf);
        return n;
    }

    @Override
    public void flush() {
        // if (!closed) {
        throw new UnsupportedOperationException();
        // }
        // upload();
    }

    @Override
    public void close() throws IOException {
        closed = true;

        this.in.close();
        this.out.close();

        upload();
    }

    private void upload() throws IOException {
        path.upload(temp);

        if (!temp.delete()) {
            throw new IOException("Error deleting file");
        }
    }

}
