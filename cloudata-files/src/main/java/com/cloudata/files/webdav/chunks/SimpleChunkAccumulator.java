package com.cloudata.files.webdav.chunks;

import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.List;

import com.cloudata.files.io.ByteBufferListInputStream;
import com.cloudata.files.io.EmptyInputStream;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;

public class SimpleChunkAccumulator implements ChunkAccumulator {
    List<ByteBuf> buffers;
    long totalSize = 0;

    File file;
    FileOutputStream os;

    long fileThresholdSize = 64 * 1024;

    @Override
    public synchronized void append(ByteBuf content) throws IOException {
        int length = content.readableBytes();

        if (file == null) {
            if (buffers == null) {
                buffers = Lists.newArrayList();
            }

            buffers.add(content);
            content.retain();

            totalSize += length;

            if (totalSize > fileThresholdSize) {
                switchToFile();
            }
        } else {
            content.readBytes(os, length);
            totalSize += content.readableBytes();
        }
    }

    public synchronized void switchToFile() throws IOException {
        if (file != null) {
            throw new IllegalStateException();
        }

        file = File.createTempFile("post", "data");
        os = new FileOutputStream(file);

        FileChannel channel = os.getChannel();

        if (buffers != null) {
            for (ByteBuf buf : buffers) {
                int n = buf.readableBytes();
                buf.readBytes(channel, n);
                buf.release();
            }

            buffers = null;
        }
    }

    @Override
    public boolean isEmpty() {
        return totalSize == 0;
    }

    public InputStream open() throws IOException {
        if (totalSize == 0) {
            return new EmptyInputStream();
        }

        if (buffers != null) {
            return new ByteBufferListInputStream(buffers);
        } else {

            return new FileInputStream(file);
        }
    }

    @Override
    public void close() throws IOException {
        if (os != null) {
            os.close();
            os = null;
        }

        if (file != null) {
            if (!file.delete()) {
                throw new IOException("Unable to delete file");
            }
            file = null;
        }

        if (buffers != null) {
            for (ByteBuf buf : buffers) {
                buf.release();
            }
        }
    }

    public ByteSource getByteSource() throws IOException {
        if (buffers != null) {
            List<ByteSource> byteSources = Lists.newArrayList();
            for (ByteBuf buf : buffers) {
                byteSources.add(new ByteBufByteSource(buf));
            }
            return ByteSource.concat(byteSources);
        } else if (file != null) {
            if (os != null) {
                os.flush();
            }

            return Files.asByteSource(file);
        } else {
            return ByteSource.empty();
        }
    }

    public boolean isInMemory() {
        return file == null;
    }

    public File getFile() throws IOException {
        if (file == null) {
            throw new IllegalStateException();
        }
        return file;
    }

    @Override
    public void end() throws IOException {
        if (os != null) {
            os.close();
            os = null;
        }
    }

}
