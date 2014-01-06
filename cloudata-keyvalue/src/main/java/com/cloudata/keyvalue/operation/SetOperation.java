package com.cloudata.keyvalue.operation;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.KeyValueLog;
import com.cloudata.keyvalue.KeyValueLog.KvAction;
import com.cloudata.keyvalue.KeyValueLog.KvEntry;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class SetOperation implements KeyOperation<Integer> {

    private final KvEntry entry;

    int addCount;

    public SetOperation(KeyValueLog.KvEntry entry) {
        Preconditions.checkArgument(entry.getAction() == KvAction.SET);

        this.entry = entry;
    }

    @Override
    public KvEntry serialize() {
        return entry;
    }

    @Override
    public Value doAction(Value oldValue) {
        if (entry.getIfNotExists()) {
            if (oldValue != null) {
                // Don't change
                return oldValue;
            }
        }

        if (entry.hasIfValue()) {
            if (!entry.getIfValue().asReadOnlyByteBuffer().equals(oldValue.asBytes())) {
                // Don't change
                return oldValue;
            }
        }

        Value newValue = Value.deserialize(entry.getValue().asReadOnlyByteBuffer());
        addCount++;
        return newValue;
    }

    @Override
    public Integer getResult() {
        return addCount;
    }

    @Override
    public ByteBuffer getKey() {
        return entry.getKey().asReadOnlyByteBuffer();
    }

    public static SetOperation build(long storeId, ByteString qualifiedKey, Value value) {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setAction(KvAction.SET);
        b.setStoreId(storeId);
        b.setKey(qualifiedKey);
        b.setValue(ByteString.copyFrom(value.serialize()));
        return new SetOperation(b.build());
    }

}
