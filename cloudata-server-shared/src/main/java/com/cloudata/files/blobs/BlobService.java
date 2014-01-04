package com.cloudata.files.blobs;

import com.google.protobuf.ByteString;

public interface BlobService {
    public BlobStore get(ByteString id);

    public ByteString allocate();

    public void delete(ByteString id);
}