package com.cloudata.keyvalue.operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;
import com.cloudata.values.Value;

public class IncrementOperation extends KeyOperation<Long> {

    private static final Logger log = LoggerFactory.getLogger(IncrementOperation.class);

    final long delta;
    private Long result;

    public IncrementOperation(long delta) {
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
        b.setAction(KvAction.INCREMENT);
        b.setIncrementBy(delta);
        return b;
    }

    @Override
    public Long getResult() {
        return result;
    }

}
