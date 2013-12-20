package com.cloudata.files.blobs;

import java.io.IOException;

import com.google.common.io.ByteSource;
import com.google.protobuf.ByteString;

public interface BlobStore {

    BlobCache.CacheFileHandle find(ByteString key) throws IOException;

    ByteString put(ByteString prefix, ByteSource source) throws IOException;

}
