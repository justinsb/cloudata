package com.cloudata.keyvalue.operation;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.KeyValueLog.KvAction;
import com.cloudata.keyvalue.KeyValueLog.KvEntry;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class AppendOperation implements KeyOperation<Integer> {

    private int newLength;
    final KvEntry entry;

    public AppendOperation(KvEntry entry) {
        Preconditions.checkState(entry.getAction() == KvAction.APPEND);
        Preconditions.checkState(!entry.getIfNotExists());
        Preconditions.checkState(!entry.hasIfValue());

        this.entry = entry;
    }

    @Override
    public Value doAction(Value oldValue) {
        Value appendValue = Value.deserialize(entry.getValue().asReadOnlyByteBuffer());

        if (oldValue == null) {
            this.newLength = entry.getValue().size();
            return appendValue;
        } else {
            Value appended = oldValue.concat(appendValue.asBytes());

            this.newLength = appended.sizeAsBytes();
            return appended;
        }
    }

    @Override
    public KvEntry serialize() {
        return entry;
    }

    @Override
    public Integer getResult() {
        return newLength;
    }

    @Override
    public ByteBuffer getKey() {
        return entry.getKey().asReadOnlyByteBuffer();
    }

    public static AppendOperation build(long storeId, ByteString qualifiedKey, Value value) {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setAction(KvAction.APPEND);
        b.setStoreId(storeId);
        b.setKey(qualifiedKey);
        b.setValue(ByteString.copyFrom(value.serialize()));
        return new AppendOperation(b.build());
    }
}
