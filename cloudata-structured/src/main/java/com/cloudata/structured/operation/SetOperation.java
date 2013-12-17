package com.cloudata.structured.operation;

import com.cloudata.structured.StructuredProto.LogAction;
import com.cloudata.structured.StructuredProto.LogEntry;
import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class SetOperation extends com.cloudata.btree.operation.SetOperation implements StructuredOperation<Void> {

    public SetOperation(Value newValue) {
        super(newValue);
    }

    @Override
    public LogEntry.Builder serialize() {
        LogEntry.Builder b = LogEntry.newBuilder();
        b.setAction(LogAction.SET);
        b.setValue(ByteString.copyFrom(newValue.serialize()));
        return b;
    }

}
