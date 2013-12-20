package com.cloudata.files.blobs;

import java.io.File;
import java.io.IOException;

import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.files.blobs.BlobCache.BlobFetcher;
import com.cloudata.files.blobs.BlobCache.CacheFileHandle;
import com.cloudata.util.TempFile;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.protobuf.ByteString;

@Singleton
public class LocalBlobStore implements BlobStore {

    private static final Logger log = LoggerFactory.getLogger(LocalBlobStore.class);

    final BlobCache cache;
    final File basePath;
    final File tempDir;

    public LocalBlobStore(BlobCache cache, File basePath) {
        this.cache = cache;
        this.basePath = basePath;
        this.tempDir = new File(basePath, "temp");

        this.basePath.mkdirs();
        this.tempDir.mkdirs();
    }

    private final BlobFetcher fetcher = new BlobFetcher() {

        @Override
        public void read(ByteString key, ByteSink byteSink) throws IOException {
            File path = toPath(key);
            path.getParentFile().mkdirs();

            ByteSource source = Files.asByteSource(path);
            source.copyTo(byteSink);
        }
    };

    @Override
    public CacheFileHandle find(ByteString key) throws IOException {
        return cache.find(key, fetcher);
    }

    protected File toPath(ByteString key) {
        return new File(basePath, BaseEncoding.base64Url().encode(key.toByteArray()));
    }

    @Override
    public ByteString put(ByteString prefix, ByteSource source) throws IOException {
        try (TempFile tempFile = new TempFile(tempDir)) {
            source.copyTo(tempFile.asByteSink());

            HashFunction hashFunction = Hashing.sha256();
            HashCode hash = tempFile.hash(hashFunction);
            ByteString key = ByteString.copyFrom(hash.asBytes());

            if (prefix != null && prefix.size() != 0) {
                key = prefix.concat(key);
            }

            File path = toPath(key);

            tempFile.renameTo(path);

            // TODO: Populate cache

            return key;
        }

    }

}
