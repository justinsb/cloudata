package com.cloudata.files.blobs;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.files.fs.FsException;
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

        CacheFileHandle(File cacheFile) {
            super();
            this.cacheFile = cacheFile;
        }

        public File getFile() {
            return cacheFile;
        }

        @Override
        public void close() {
            // TODO: Release lock
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
                throw new FsException("Error reading blob", e);
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
