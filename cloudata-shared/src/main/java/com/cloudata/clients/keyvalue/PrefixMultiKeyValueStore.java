//package com.cloudata.clients.keyvalue;
//
//import java.nio.ByteBuffer;
//
//import com.google.protobuf.ByteString;
//
//public class PrefixMultiKeyValueStore implements MultiKeyValueStore {
//    final KeyValueStore keyValueStore;
//
//    public PrefixMultiKeyValueStore(KeyValueStore keyValueStore) {
//        this.keyValueStore = keyValueStore;
//    }
//
//    @Override
//    public KeyValueStore get(short id) {
//        ByteBuffer b = ByteBuffer.allocate(2);
//        b.putShort(id);
//        b.flip();
//
//        ByteString prefix = ByteString.copyFrom(b);
//
//        return PrefixKeyValueStore.create(keyValueStore, prefix);
//    }
//
// }
