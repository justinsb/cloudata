package com.cloudata.btree.operation;

import java.nio.ByteBuffer;

import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class SimpleSetOperation implements RowOperation<Void> {

    private final ByteString key;
    private final Value newValue;

    public SimpleSetOperation(ByteString key, Value newValue) {
        this.key = key;
        this.newValue = newValue;
    }

    @Override
    public Value doAction(Value oldValue) {
        return newValue;
    }

    @Override
    public Void getResult() {
        return null;
    }

    @Override
    public ByteBuffer getKey() {
        return key.asReadOnlyByteBuffer();
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
}
