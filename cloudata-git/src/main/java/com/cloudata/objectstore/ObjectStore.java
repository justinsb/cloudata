package com.cloudata.objectstore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.protobuf.ByteString;

public interface ObjectStore {

    void delete(String path) throws IOException;

    List<ObjectInfo> listChildren(String path, boolean recurse) throws IOException;

    ObjectInfo getInfo(String path) throws IOException;

    void upload(String path, File src) throws IOException;

    void upload(String path, ByteString data) throws IOException;

    long read(String path, ByteBuffer dst, long position) throws IOException;

    ByteString read(String path) throws IOException;

}
