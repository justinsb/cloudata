package com.cloudata.keyvalue.btree.operation;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.btree.ByteBuffers;

public class IncrementOperation extends KeyOperation {

    final long delta;

    public IncrementOperation(long delta) {
        this.delta = delta;
    }

    @Override
    public ByteBuffer doAction(ByteBuffer oldValue) {
        long oldValueLong = 0;

        if (oldValue != null) {
            oldValueLong = ByteBuffers.parseLong(oldValue);
        }

        oldValueLong += delta;

        ByteBuffer newValue = ByteBuffer.wrap(Long.toString(oldValueLong).getBytes());

        return newValue;
    }

}
