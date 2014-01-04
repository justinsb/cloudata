package com.cloudata.files.blobs;

import java.io.IOException;

import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.files.blobs.BlobCache.BlobFetcher;
import com.cloudata.files.blobs.BlobCache.CacheFileHandle;
import com.cloudata.objectstore.ObjectStore;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

@Singleton
public class CloudObjectStoreBlobStore implements BlobStore {

    private static final Logger log = LoggerFactory.getLogger(CloudObjectStoreBlobStore.class);

    final BlobCache cache;
    final ObjectStore objectStore;
    final String basePath;

    public CloudObjectStoreBlobStore(BlobCache cache, ObjectStore objectStore, String basePath) {
        this.cache = cache;
        this.objectStore = objectStore;
        this.basePath = basePath;
    }

    private final BlobFetcher fetcher = new BlobFetcher() {

        @Override
        public void read(ByteString key, ByteSink byteSink) throws IOException {
            String path = toPath(key);
            objectStore.read(path, byteSink);
        }
    };

    @Override
    public ListenableFuture<CacheFileHandle> find(ByteString key) {
        throw new UnsupportedOperationException();
        // return cache.find(key, fetcher);
    }

    protected String toPath(ByteString key) {
        return basePath + BaseEncoding.base64Url().encode(key.toByteArray());
    }

    @Override
    public ListenableFuture<ByteString> put(ByteString prefix, ByteSource source) {
        throw new UnsupportedOperationException();
    }

}
