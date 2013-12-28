package com.cloudata.btree.operation;

import java.nio.ByteBuffer;

import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class SetOperation implements RowOperation<Void> {

    protected final ByteString key;
    protected final Value newValue;

    public SetOperation(ByteString key, Value newValue) {
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
}
