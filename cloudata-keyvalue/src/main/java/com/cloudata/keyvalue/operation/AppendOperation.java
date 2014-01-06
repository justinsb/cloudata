package com.cloudata.keyvalue.operation;

import com.cloudata.btree.Keyspace;
import com.cloudata.keyvalue.KeyValueProtocol.ActionType;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueAction;
import com.cloudata.keyvalue.KeyValueProtocol.ResponseEntry;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class AppendOperation extends KeyValueOperationBase {

    public AppendOperation(KeyValueAction entry) {
        super(entry);

        Preconditions.checkState(entry.getAction() == ActionType.APPEND);
        Preconditions.checkState(!entry.getIfNotExists());
        Preconditions.checkState(!entry.hasIfValue());
    }

    @Override
    public Value doAction(Value oldValue) {
        Value appendValue = Value.fromRawBytes(entry.getValue());

        ResponseEntry.Builder eb = response.addEntryBuilder();
        eb.setKey(entry.getKey());

        Value newValue;
        if (oldValue == null) {
            newValue = appendValue;
        } else {
            newValue = oldValue.concat(appendValue.asBytes());
        }
        eb.setValue(ByteString.copyFrom(newValue.asBytes()));
        eb.setChanged(true);

        return newValue;
    }

    public static AppendOperation build(long storeId, Keyspace keyspace, ByteString key, Value value) {
        KeyValueAction.Builder b = KeyValueAction.newBuilder();
        b.setAction(ActionType.APPEND);
        b.setStoreId(storeId);
        b.setKeyspaceId(keyspace.getKeyspaceId());
        b.setKey(key);
        b.setValue(ByteString.copyFrom(value.asBytes()));
        return new AppendOperation(b.build());
    }
}
