package com.cloudata.structured.operation;

import com.cloudata.structured.StructuredProto.LogAction;
import com.cloudata.structured.StructuredProto.LogEntry;

public class DeleteOperation extends com.cloudata.btree.operation.DeleteOperation implements
        StructuredOperation<Integer> {

    @Override
    public LogEntry.Builder serialize() {
        LogEntry.Builder b = LogEntry.newBuilder();
        b.setAction(LogAction.DELETE);
        return b;
    }
}
