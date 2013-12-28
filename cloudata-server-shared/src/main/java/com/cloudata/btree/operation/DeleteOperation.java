package com.cloudata.btree.operation;

import java.nio.ByteBuffer;

import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class DeleteOperation implements RowOperation<Integer> {

    protected final ByteString key;

    public DeleteOperation(ByteString key) {
        this.key = key;
    }

    private int deleteCount;

    @Override
    public Value doAction(Value oldValue) {
        if (oldValue != null) {
            deleteCount++;
        }
        return null; // Delete the value
    }

    @Override
    public Integer getResult() {
        return deleteCount;
    }

    @Override
    public ByteBuffer getKey() {
        return key.asReadOnlyByteBuffer();
    }
}
