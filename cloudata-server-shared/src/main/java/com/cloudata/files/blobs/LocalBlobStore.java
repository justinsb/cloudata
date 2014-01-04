package com.cloudata.files.blobs;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.ByteString;

@Singleton
public class LocalBlobStore implements BlobStore {
    private static final Logger log = LoggerFactory.getLogger(LocalBlobStore.class);

    final BlobCache cache;
    final File basePath;
    final File tempDir;

    final ListeningExecutorService executor;

    public LocalBlobStore(ListeningExecutorService executor, BlobCache cache, File basePath) {
        this.executor = executor;
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
    public ListenableFuture<CacheFileHandle> find(final ByteString key) {
        return executor.submit(new Callable<CacheFileHandle>() {
            @Override
            public CacheFileHandle call() throws Exception {
                return cache.find(key, fetcher);
            }
        });
    }

    protected File toPath(ByteString key) {
        return new File(basePath, BaseEncoding.base64Url().encode(key.toByteArray()));
    }

    @Override
    public ListenableFuture<ByteString> put(final ByteString prefix, final ByteSource source) {
        return executor.submit(new Callable<ByteString>() {

            @Override
            public ByteString call() throws Exception {
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

        });

    }

}
