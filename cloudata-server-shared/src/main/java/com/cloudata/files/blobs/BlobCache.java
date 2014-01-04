package com.cloudata.files.blobs;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.util.TempFile;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteSink;
import com.google.protobuf.ByteString;

public class BlobCache {
    private static final Logger log = LoggerFactory.getLogger(BlobCache.class);

    final File tempDir;
    final File cacheDir;

    public BlobCache(File cacheDir) {
        this.tempDir = new File(cacheDir, "temp");
        this.cacheDir = cacheDir;

        cacheDir.mkdirs();
        tempDir.mkdirs();
    }

    public class CacheFileHandle implements Closeable {
        final File cacheFile;
        final MappedByteBuffer mmap;

        CacheFileHandle(File cacheFile) throws IOException {
            this.cacheFile = cacheFile;
            long size = cacheFile.length();
            try (RandomAccessFile raf = new RandomAccessFile(cacheFile, "r")) {
                this.mmap = raf.getChannel().map(MapMode.READ_ONLY, 0, size);
            }
        }

        public File getFile() {
            return cacheFile;
        }

        @Override
        public void close() {
            // TODO: Release lock
        }

        public ByteBuffer getData() {
            return mmap;
        }

    }

    public interface BlobFetcher {

        void read(ByteString key, ByteSink byteSink) throws IOException;

    }

    public CacheFileHandle find(ByteString key, BlobFetcher fetcher) throws IOException {
        File cacheFile = buildCacheFile(key);
        if (!cacheFile.exists()) {
            // TODO: Avoid concurrent downloading??

            try (TempFile tempFile = new TempFile(tempDir)) {
                try {
                    fetcher.read(key, tempFile.asByteSink());
                } catch (FileNotFoundException e) {
                    return null;
                }

                cacheFile.getParentFile().mkdirs();

                tempFile.renameTo(cacheFile);
            } catch (IOException e) {
                throw new IOException("Error reading blob", e);
            }
        }

        return new CacheFileHandle(cacheFile);
    }

    private File buildCacheFile(ByteString hash) {
        String hex = BaseEncoding.base16().encode(hash.toByteArray());
        String path = hex.substring(0, 3) + File.separator + hex.substring(3, 5) + File.separator + hex;
        File cacheFile = new File(cacheDir, path);

        if (!cacheFile.getParentFile().exists()) {
            cacheFile.getParentFile().mkdirs();
        }

        return cacheFile;
    }

}
