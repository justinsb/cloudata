package com.cloudata.blockstore.operation;

import com.cloudata.blockstore.IscsiProto.LogEntry;
import com.cloudata.btree.operation.BtreeOperation;

public interface BlockStoreOperation<V> extends BtreeOperation<V> {

    public abstract LogEntry.Builder serialize();
}
