package com.cloudata.btree.operation;

import java.nio.ByteBuffer;

import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class SimpleDeleteOperation implements RowOperation<Integer> {

    protected final ByteString key;

    int deleteCount;

    public SimpleDeleteOperation(ByteString key) {
        this.key = key;
    }

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

    @Override
    public boolean isReadOnly() {
        return false;
    }
}
