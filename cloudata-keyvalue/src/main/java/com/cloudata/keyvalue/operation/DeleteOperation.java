package com.cloudata.keyvalue.operation;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.KeyValueLog.KvAction;
import com.cloudata.keyvalue.KeyValueLog.KvEntry;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class DeleteOperation implements KeyOperation<Integer> {

    private final KvEntry entry;

    public DeleteOperation(KvEntry entry) {
        Preconditions.checkState(entry.getAction() == KvAction.DELETE);
        this.entry = entry;
    }

    private int deleteCount;

    @Override
    public Value doAction(Value oldValue) {
        if (entry.hasIfValue()) {
            boolean match = true;
            if (oldValue == null) {
                match = false;
            } else if (!entry.getIfValue().asReadOnlyByteBuffer().equals(oldValue.asBytes())) {
                match = false;
            }

            if (!match) {
                // Mismatch on value-conditional; don't change value
                return oldValue;
            }
        }

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
        return entry.getKey().asReadOnlyByteBuffer();
    }

    public static DeleteOperation build(long storeId, ByteString qualifiedKey) {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setStoreId(storeId);
        b.setAction(KvAction.DELETE);
        b.setKey(qualifiedKey);
        return new DeleteOperation(b.build());
    }

    @Override
    public KvEntry serialize() {
        return entry;
    }

}
