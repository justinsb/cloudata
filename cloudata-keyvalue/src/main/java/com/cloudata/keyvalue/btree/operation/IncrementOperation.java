package com.cloudata.keyvalue.btree.operation;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;
import com.cloudata.keyvalue.btree.ByteBuffers;

public class IncrementOperation extends KeyOperation<ByteBuffer> {

    final long delta;
    private ByteBuffer newValue;

    public IncrementOperation(long delta) {
        this.delta = delta;
    }

    @Override
    public ByteBuffer doAction(ByteBuffer oldValue) {
        long oldValueLong = 0;

        if (oldValue != null) {
            oldValueLong = ByteBuffers.parseLong(oldValue);
        }

        long newValueLong = oldValueLong + delta;

        ByteBuffer newValue = ByteBuffer.wrap(Long.toString(newValueLong).getBytes());

        this.newValue = newValue.duplicate();

        // this.newValueLong = newValueLong;

        return newValue;
    }

    @Override
    public KvEntry.Builder serialize() {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setAction(KvAction.INCREMENT);
        b.setIncrementBy(delta);
        return b;
    }

    @Override
    public ByteBuffer getResult() {
        return newValue;
    }

}
