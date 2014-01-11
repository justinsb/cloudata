package com.cloudata.structured.operation;

import com.cloudata.btree.ByteBuffers;
import com.cloudata.structured.StructuredProtocol.ActionResponseCode;
import com.cloudata.structured.StructuredProtocol.StructuredAction;
import com.cloudata.structured.StructuredProtocol.StructuredActionType;
import com.cloudata.structured.StructuredProtocol.StructuredResponseEntry;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

public class StructuredDeleteOperation extends SimpleStructuredOperationBase {
    public StructuredDeleteOperation(StructuredAction action) {
        super(action);

        Preconditions.checkState(action.getAction() == StructuredActionType.STRUCTURED_DELETE);
        Preconditions.checkArgument(!action.getIfNotExists());
        Preconditions.checkArgument(!action.hasValue());

        Preconditions.checkArgument(!keyspace.isSystem());
    }

    @Override
    public Value doAction(Value oldValue) {
        StructuredResponseEntry.Builder eb = response.addEntryBuilder();
        if (returnKeys) {
            eb.setKey(action.getKey());
        }

        if (oldValue == null) {
            eb.setCode(ActionResponseCode.NOT_FOUND);
            return oldValue; // Doesn't exist; no change
        }

        if (action.hasIfVersion()) {
            boolean match = true;

            long oldValueVersion = ByteBuffers.hash(Hashing.md5(), oldValue.asBytes()).asLong();

            if (action.getIfVersion() != oldValueVersion) {
                match = false;
            }

            if (!match) {
                // Mismatch on value-conditional; don't change value
                eb.setCode(ActionResponseCode.VERSION_MISMATCH);
                return oldValue;
            }
        }

        if (returnValues) {
            eb.setValue(ByteString.copyFrom(oldValue.asBytes()));
        }

        eb.setCode(ActionResponseCode.DONE);
        return null; // Delete the value
    }

    // public static StructuredDeleteOperation build(long storeId, int tablespaceId, ByteString key) {
    // StructuredAction.Builder b = StructuredAction.newBuilder();
    // b.setStoreId(storeId);
    // b.setTablespaceId(tablespaceId);
    // b.setKey(key);
    // b.setAction(StructuredActionType.STRUCTURED_DELETE);
    // return new StructuredDeleteOperation(b.build());
    // }

    public static StructuredDeleteOperation build(long storeId, int keyspaceId, ByteString key) {
        StructuredAction.Builder b = StructuredAction.newBuilder();
        b.setStoreId(storeId);
        b.setKeyspaceId(keyspaceId);
        b.setKey(key);
        b.setAction(StructuredActionType.STRUCTURED_DELETE);
        return new StructuredDeleteOperation(b.build());
    }
}
