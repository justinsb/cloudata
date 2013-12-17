package com.cloudata.structured.operation;

import com.cloudata.btree.BtreeOperation;
import com.cloudata.structured.StructuredProto.LogEntry;
import com.cloudata.values.Value;

public abstract class StructuredOperation<V> implements BtreeOperation<V> {
    @Override
    public abstract Value doAction(Value oldValue);

    public abstract LogEntry.Builder serialize();

    @Override
    public abstract V getResult();
}
