package com.cloudata.keyvalue.operation;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;
import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class IncrementOperation implements KeyOperation<Long> {

    private static final Logger log = LoggerFactory.getLogger(IncrementOperation.class);

    final long delta;
    private Long result;

    final ByteString key;

    public IncrementOperation(ByteString key, long delta) {
        this.key = key;
        this.delta = delta;
    }

    @Override
    public Value doAction(Value oldValue) {
        long oldValueLong = 0;

        if (oldValue != null) {
            oldValueLong = oldValue.asLong();
        }

        long newValueLong = oldValueLong + delta;

        Value newValue = Value.fromLong(newValueLong);

        log.debug("Increment: {} -> {}", oldValueLong, newValueLong);

        // this.newValue = newValue.duplicate();

        this.result = newValueLong;

        return newValue;
    }

    @Override
    public KvEntry.Builder serialize() {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setKey(key);
        b.setAction(KvAction.INCREMENT);
        b.setIncrementBy(delta);
        return b;
    }

    @Override
    public Long getResult() {
        return result;
    }

    @Override
    public ByteBuffer getKey() {
        return key.asReadOnlyByteBuffer();
    }

}
