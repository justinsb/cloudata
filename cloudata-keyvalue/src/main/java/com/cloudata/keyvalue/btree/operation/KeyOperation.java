package com.cloudata.keyvalue.btree.operation;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.KeyValueProto.KvEntry;

public abstract class KeyOperation<V> {
    public abstract ByteBuffer doAction(ByteBuffer oldValue);

    public abstract KvEntry.Builder serialize();

    public abstract V getResult();
}
