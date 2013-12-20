//package com.cloudata.objectstore;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.util.List;
//
//import com.google.protobuf.ByteString;
//import com.woorea.openstack.swift.Swift;
//import com.woorea.openstack.swift.model.ObjectForUpload;
//
//public class OpenstackObjectStore implements ObjectStore {
//
//    final Swift swift;
//
//    @Override
//    public void delete(String path) throws IOException {
//        SwiftPath swiftPath = SwiftPath.split(path);
//        swift.containers().container(swiftPath.container).delete(swiftPath.object).execute();
//    }
//
//    @Override
//    public List<ObjectInfo> listChildren(String path, boolean recurse) throws IOException {
//    }
//
//    @Override
//    public ObjectInfo getInfo(String path) throws IOException {
//    }
//
//    @Override
//    public void upload(String path, File src) throws IOException {
//        SwiftPath swiftPath = SwiftPath.split(path);
//        ObjectForUpload objectForUpload = new ObjectForUpload();
//        objectForUpload.setName(swiftPath.object);
//        try (objectForUpload.setInputStream(is);
//        swift.containers().container(swiftPath.container).upload(objectForUpload).execute();
//    }
//
//    @Override
//    public long read(ByteBuffer dst, long position) throws IOException {
//    }
//
//    @Override
//    public void create(ByteString data) throws IOException {
//todo
//    }
//
//    @Override
//    public ByteString read() throws IOException {
//    }
//
// }
