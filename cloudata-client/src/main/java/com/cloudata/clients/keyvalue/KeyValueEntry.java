package com.cloudata.clients.keyvalue;

import com.google.protobuf.ByteString;

public class KeyValueEntry {
    final ByteString key;
    final ByteString value;

    public KeyValueEntry(ByteString key, ByteString value) {
        this.key = key;
        this.value = value;
    }

    public ByteString getKey() {
        return key;
    }

    public ByteString getValue() {
        return value;
    }

}
