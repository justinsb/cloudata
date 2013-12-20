package com.cloudata.objectstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.MutableBlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.io.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;

public class JCloudsObjectStore implements ObjectStore {

    private static final Logger log = LoggerFactory.getLogger(JCloudsObjectStore.class);

    final BlobStore blobStore;

    public JCloudsObjectStore(BlobStore blobStore) {
        this.blobStore = blobStore;
    }

    @Override
    public void delete(String path) throws IOException {
        SwiftPath swiftPath = SwiftPath.split(path);
        blobStore.removeBlob(swiftPath.container, swiftPath.name);
    }

    @Override
    public List<ObjectInfo> listChildren(String path, boolean recurse) throws IOException {
        SwiftPath swiftPath = SwiftPath.split(path);
        ListContainerOptions options = ListContainerOptions.Builder.inDirectory(swiftPath.name);
        if (recurse) {
            options = options.recursive();
        }

        List<ObjectInfo> children = Lists.newArrayList();

        String marker;
        do {
            PageSet<? extends StorageMetadata> pageSet = blobStore.list(swiftPath.container, options);

            for (StorageMetadata i : pageSet) {
                StorageType type = i.getType();
                switch (type) {
                case BLOB:
                    children.add(new JCloudsFileInfo(swiftPath.container, (BlobMetadata) i));
                    break;

                case FOLDER:
                    children.add(new JCloudsDirInfo(swiftPath.container, i));
                    break;

                default:
                    throw new IllegalStateException();
                }
            }
            marker = pageSet.getNextMarker();
        } while (marker != null);

        return children;
    }

    @Override
    public ObjectInfo getInfo(String path) throws IOException {
        SwiftPath swiftPath = SwiftPath.split(path);
        BlobMetadata blobMetadata = blobStore.blobMetadata(swiftPath.container, swiftPath.name);
        if (blobMetadata == null) {
            return null;
        }

        return new JCloudsFileInfo(swiftPath.container, blobMetadata);
    }

    @Override
    public void upload(String path, File src) throws IOException {
        SwiftPath swiftPath = SwiftPath.split(path);
        Blob blob = blobStore.blobBuilder(swiftPath.name).payload(src).build();
        blobStore.putBlob(swiftPath.container, blob);
    }

    @Override
    public long read(String path, ByteBuffer dst, long readOffset) throws IOException {
        SwiftPath swiftPath = SwiftPath.split(path);
        GetOptions options = GetOptions.Builder.range(readOffset, readOffset + dst.remaining() - 1);

        log.debug("Doing ObjectStorage GET for {} {}", path, options.getRanges());

        Blob blob = blobStore.getBlob(swiftPath.container, swiftPath.name, options);
        if (blob == null) {
            throw new FileNotFoundException();
        }

        ByteBufferOutputStream os = new ByteBufferOutputStream(dst);

        try (Payload payload = blob.getPayload()) {
            try (InputStream is = payload.getInput()) {
                long n = ByteStreams.copy(is, os);
                return n;
            }
        }
    }

    @Override
    public void upload(String path, ByteString data) throws IOException {
        SwiftPath swiftPath = SwiftPath.split(path);
        Blob blob = blobStore.blobBuilder(swiftPath.name).payload(data.toByteArray()).build();
        blobStore.putBlob(swiftPath.container, blob);
    }

    @Override
    public ByteString read(String path) throws IOException {
        SwiftPath swiftPath = SwiftPath.split(path);
        Blob blob = blobStore.getBlob(swiftPath.container, swiftPath.name);
        if (blob == null) {
            throw new FileNotFoundException();
        }

        try (Payload payload = blob.getPayload()) {
            try (InputStream is = payload.getInput()) {
                return ByteString.readFrom(is);
            }
        }
    }

    @Override
    public ObjectInfo read(String path, ByteSink sink) throws IOException {
        SwiftPath swiftPath = SwiftPath.split(path);
        Blob blob = blobStore.getBlob(swiftPath.container, swiftPath.name);
        if (blob == null) {
            throw new FileNotFoundException();
        }

        try (Payload payload = blob.getPayload()) {
            try (InputStream is = payload.getInput()) {
                ByteStreams.copy(is, sink);
            }
        }

        MutableBlobMetadata blobMetadata = blob.getMetadata();

        return new JCloudsFileInfo(swiftPath.container, blobMetadata);
    }
}
