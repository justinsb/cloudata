package com.cloudata.structured.operation;

import com.cloudata.structured.StructuredProto.LogAction;
import com.cloudata.structured.StructuredProto.LogEntry;
import com.cloudata.values.Value;

public class DeleteOperation implements StructuredOperation<Integer> {

    private int deleteCount;

    @Override
    public Value doAction(Value oldValue) {
        if (oldValue != null) {
            deleteCount++;
        }
        return null; // Delete the value
    }

    @Override
    public LogEntry.Builder serialize() {
        LogEntry.Builder b = LogEntry.newBuilder();
        b.setAction(LogAction.DELETE);
        return b;
    }

    @Override
    public Integer getResult() {
        return deleteCount;
    }
}
