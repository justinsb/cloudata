package com.cloudata.keyvalue.operation;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.KeyValueLog.KvAction;
import com.cloudata.keyvalue.KeyValueLog.KvEntry;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class IncrementOperation implements KeyOperation<Long> {

    private static final Logger log = LoggerFactory.getLogger(IncrementOperation.class);

    private Long result;

    private final KvEntry entry;

    public IncrementOperation(KvEntry entry) {
        Preconditions.checkArgument(entry.getAction() == KvAction.INCREMENT);
        Preconditions.checkArgument(entry.hasIncrementBy());
        this.entry = entry;
    }

    @Override
    public Value doAction(Value oldValue) {
        long oldValueLong = 0;

        if (oldValue != null) {
            oldValueLong = oldValue.asLong();
        }

        long newValueLong = oldValueLong + entry.getIncrementBy();

        Value newValue = Value.fromLong(newValueLong);

        log.debug("Increment: {} -> {}", oldValueLong, newValueLong);

        // this.newValue = newValue.duplicate();

        this.result = newValueLong;

        return newValue;
    }

    @Override
    public KvEntry serialize() {
        return entry;
    }

    @Override
    public Long getResult() {
        return result;
    }

    @Override
    public ByteBuffer getKey() {
        return entry.getKey().asReadOnlyByteBuffer();
    }

    public static IncrementOperation build(long storeId, ByteString qualifiedKey, long delta) {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setAction(KvAction.INCREMENT);
        b.setStoreId(storeId);
        b.setKey(qualifiedKey);
        b.setIncrementBy(delta);
        return new IncrementOperation(b.build());
    }

}
