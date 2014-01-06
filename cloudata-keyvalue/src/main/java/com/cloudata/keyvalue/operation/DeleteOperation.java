package com.cloudata.keyvalue.operation;

import com.cloudata.btree.Keyspace;
import com.cloudata.keyvalue.KeyValueProtocol.ActionType;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueAction;
import com.cloudata.keyvalue.KeyValueProtocol.ResponseEntry;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class DeleteOperation extends KeyValueOperationBase {

    public DeleteOperation(KeyValueAction entry) {
        super(entry);

        Preconditions.checkState(entry.getAction() == ActionType.DELETE);
        Preconditions.checkArgument(!entry.getIfNotExists());
        Preconditions.checkArgument(!entry.hasValue());
    }

    @Override
    public Value doAction(Value oldValue) {
        if (oldValue == null) {
            return oldValue; // No change
        }

        ResponseEntry.Builder eb = response.addEntryBuilder();
        eb.setKey(entry.getKey());

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
            eb.setChanged(true);
        }

        return null; // Delete the value
    }

    public static DeleteOperation build(long storeId, Keyspace keyspace, ByteString key) {
        KeyValueAction.Builder b = KeyValueAction.newBuilder();
        b.setStoreId(storeId);
        b.setKeyspaceId(keyspace.getKeyspaceId());
        b.setKey(key);
        b.setAction(ActionType.DELETE);
        return new DeleteOperation(b.build());
    }

}
