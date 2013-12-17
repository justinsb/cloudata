package com.cloudata.structured.operation;

import com.cloudata.structured.StructuredProto.LogAction;
import com.cloudata.structured.StructuredProto.LogEntry;
import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class SetOperation extends StructuredOperation<Void> {

    final Value newValue;

    public SetOperation(Value newValue) {
        this.newValue = newValue;
    }

    @Override
    public Value doAction(Value oldValue) {
        return newValue;
    }

    @Override
    public LogEntry.Builder serialize() {
        LogEntry.Builder b = LogEntry.newBuilder();
        b.setAction(LogAction.SET);
        b.setValue(ByteString.copyFrom(newValue.serialize()));
        return b;
    }

    @Override
    public Void getResult() {
        return null;
    }
}
