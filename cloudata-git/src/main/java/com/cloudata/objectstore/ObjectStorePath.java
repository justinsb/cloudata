package com.cloudata.objectstore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.protobuf.ByteString;

public class ObjectStorePath {
    private static final String SEPARATOR = "/";
    final ObjectStore store;
    final String path;

    public ObjectStorePath(ObjectStore store, String path) {
        this.store = store;
        this.path = path;
    }

    public List<ObjectInfo> listChildren(boolean recurse) throws IOException {
        return store.listChildren(path, recurse);
    }

    public void delete() throws IOException {
        store.delete(path);
    }

    public ObjectStorePath child(String add) {
        String childPath = path;
        if (!childPath.endsWith(SEPARATOR)) {
            childPath += SEPARATOR;
        }
        childPath += add;
        return new ObjectStorePath(store, childPath);
    }

    public ObjectInfo getInfo() throws IOException {
        return store.getInfo(path);
    }

    public void upload(File src) throws IOException {
        store.upload(path, src);
    }

    public long read(ByteBuffer dst, long position) throws IOException {
        return store.read(path, dst, position);
    }

    public String getPath() {
        return path;
    }

    public void create(ByteString data) throws IOException {
        store.upload(path, data);
    }

    public ByteString read() throws IOException {
        return store.read(path);
    }

}
