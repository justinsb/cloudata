package com.cloudata.keyvalue.operation;

import com.cloudata.btree.BtreeOperation;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;
import com.cloudata.values.Value;

public abstract class KeyOperation<V> implements BtreeOperation<V> {
    public abstract Value doAction(Value oldValue);

    public abstract KvEntry.Builder serialize();

    public abstract V getResult();
}
