package com.cloudata.files.blobs;

import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

public interface BlobStore {

    public interface WriteOperation {

        ListenableFuture<ByteString> getKey();

    }

    ListenableFuture<BlobCache.CacheFileHandle> find(ByteString key);

    ListenableFuture<ByteString> put(ByteString prefix, ByteSource source);

}
