package com.cloudata.keyvalue.operation;

import com.cloudata.btree.BtreeOperation;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;

public interface KeyOperation<V> extends BtreeOperation<V> {

    public abstract KvEntry.Builder serialize();
}
