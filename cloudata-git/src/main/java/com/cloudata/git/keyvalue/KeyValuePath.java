package com.cloudata.git.keyvalue;

import com.google.protobuf.ByteString;

public class KeyValuePath {
    public final KeyValueStore store;
    public final ByteString key;

    public KeyValuePath(KeyValueStore store, ByteString key) {
        this.store = store;
        this.key = key;
    }

    public KeyValuePath child(ByteString suffix) {
        ByteString child = this.key.concat(suffix);
        return new KeyValuePath(store, child);
    }

}
