package com.cloudata.keyvalue.operation;

import com.cloudata.btree.Keyspace;
import com.cloudata.keyvalue.KeyValueProtocol.ActionType;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueAction;
import com.cloudata.keyvalue.KeyValueProtocol.ResponseEntry;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class GetOperation extends KeyValueOperationBase {

    public GetOperation(KeyValueAction entry) {
        super(entry);

        Preconditions.checkState(entry.getAction() == ActionType.GET);
        Preconditions.checkState(!entry.getIfNotExists());
        Preconditions.checkState(!entry.hasIfValue());
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public Value doAction(Value oldValue) {
        if (oldValue != null) {
            ResponseEntry.Builder eb = response.addEntryBuilder();
            eb.setKey(entry.getKey());
            eb.setValue(ByteString.copyFrom(oldValue.asBytes()));
        }
        return oldValue;
    }

    public static GetOperation build(long storeId, Keyspace keyspace, ByteString key) {
        KeyValueAction.Builder b = KeyValueAction.newBuilder();
        b.setAction(ActionType.APPEND);
        b.setStoreId(storeId);
        b.setKeyspaceId(keyspace.getKeyspaceId());
        b.setKey(key);
        return new GetOperation(b.build());
    }

}
