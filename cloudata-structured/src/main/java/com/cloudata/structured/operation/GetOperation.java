package com.cloudata.structured.operation;

import com.cloudata.btree.Keyspace;
import com.cloudata.structured.StructuredProtocol.ActionResponseCode;
import com.cloudata.structured.StructuredProtocol.StructuredAction;
import com.cloudata.structured.StructuredProtocol.StructuredActionType;
import com.cloudata.structured.StructuredProtocol.StructuredResponseEntry;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class GetOperation extends SimpleStructuredOperationBase {

    public GetOperation(StructuredAction action) {
        super(action);

        Preconditions.checkState(action.getAction() == StructuredActionType.STRUCTURED_GET);
        Preconditions.checkState(!action.getIfNotExists());
        Preconditions.checkState(!action.hasIfVersion());
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public Value doAction(Value oldValue) {
        StructuredResponseEntry.Builder eb = response.addEntryBuilder();
        if (oldValue != null) {
            if (returnKeys) {
                eb.setKey(action.getKey());
            }
            if (returnValues) {
                eb.setValue(ByteString.copyFrom(oldValue.asBytes()));
            }
            eb.setCode(ActionResponseCode.DONE);
        } else {
            eb.setCode(ActionResponseCode.NOT_FOUND);
        }
        return oldValue;
    }

    public static GetOperation build(long storeId, Keyspace keyspace, ByteString key) {
        StructuredAction.Builder b = StructuredAction.newBuilder();
        b.setAction(StructuredActionType.STRUCTURED_GET);
        b.setStoreId(storeId);
        b.setKeyspaceId(keyspace.getKeyspaceId());
        b.setKey(key);
        return new GetOperation(b.build());
    }

}
