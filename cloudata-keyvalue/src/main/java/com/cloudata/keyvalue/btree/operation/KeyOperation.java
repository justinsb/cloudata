package com.cloudata.keyvalue.btree.operation;

import com.cloudata.keyvalue.KeyValueProto.KvEntry;

public abstract class KeyOperation<V> {
    public abstract Value doAction(Value oldValue);

    public abstract KvEntry.Builder serialize();

    public abstract V getResult();
}
