package com.cloudata.files.blobs;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.ByteString;

public class LocalBlobService implements BlobService {

    final ListeningExecutorService executor;
    final File dataBase;
    final File cacheBase;

    public LocalBlobService(ListeningExecutorService executor, File basePath) {
        this.executor = executor;
        this.dataBase = new File(basePath, "data");
        this.cacheBase = new File(basePath, "cache");
    }

    @Override
    public BlobStore get(ByteString id) {
        File dataPath = new File(this.dataBase, BaseEncoding.base16().encode(id.toByteArray()));
        dataPath.mkdirs();

        File cachePath = new File(this.cacheBase, BaseEncoding.base16().encode(id.toByteArray()));
        cachePath.mkdirs();

        BlobCache cache = new BlobCache(cacheBase);

        return new LocalBlobStore(executor, cache, dataPath);
    }

    @Override
    public ByteString allocate() {
        // TODO: Verify not already allocated; allocate more densely?
        UUID uuid = UUID.randomUUID();

        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getLeastSignificantBits());
        b.putLong(uuid.getMostSignificantBits());
        b.flip();

        return ByteString.copyFrom(b);
    }

    @Override
    public void delete(ByteString id) {
        throw new UnsupportedOperationException();
    }

}
