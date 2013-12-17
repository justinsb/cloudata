package com.cloudata.structured.operation;

import com.cloudata.btree.BtreeOperation;
import com.cloudata.structured.StructuredProto.LogEntry;

public interface StructuredOperation<V> extends BtreeOperation<V> {
    LogEntry.Builder serialize();

}
