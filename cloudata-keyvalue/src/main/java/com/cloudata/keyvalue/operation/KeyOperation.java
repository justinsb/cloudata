package com.cloudata.keyvalue.operation;

import com.cloudata.btree.operation.RowOperation;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;

public interface KeyOperation<V> extends RowOperation<V> {

    public abstract KvEntry.Builder serialize();
}
