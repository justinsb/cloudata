package com.cloudata.keyvalue.operation;

import com.cloudata.btree.Keyspace;
import com.cloudata.keyvalue.KeyValueProtocol.ActionType;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueAction;
import com.cloudata.keyvalue.KeyValueProtocol.ResponseEntry.Builder;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class SetOperation extends KeyValueOperationBase {

    public SetOperation(KeyValueAction entry) {
        super(entry);

        Preconditions.checkArgument(entry.getAction() == ActionType.SET);
    }

    @Override
    public Value doAction(Value oldValue) {
        Builder eb = response.addEntryBuilder();
        eb.setKey(entry.getKey());

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

        eb.setChanged(true);

        Value newValue = Value.fromRawBytes(entry.getValue());
        return newValue;
    }

    public static SetOperation build(long storeId, Keyspace keyspace, ByteString key, Value value) {
        KeyValueAction.Builder b = KeyValueAction.newBuilder();
        b.setAction(ActionType.SET);
        b.setStoreId(storeId);
        b.setKeyspaceId(keyspace.getKeyspaceId());
        b.setKey(key);
        b.setValue(ByteString.copyFrom(value.asBytes()));
        return new SetOperation(b.build());
    }

}
