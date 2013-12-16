package com.cloudata.keyvalue.btree;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.btree.operation.Value;

public interface EntryListener {

    public boolean found(ByteBuffer key, Value value);
}
