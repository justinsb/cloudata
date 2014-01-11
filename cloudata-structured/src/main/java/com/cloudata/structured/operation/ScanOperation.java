package com.cloudata.structured.operation;

import java.nio.ByteBuffer;

import com.cloudata.btree.Btree;
import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.BtreeQuery.KeyValueResultset;
import com.cloudata.btree.EntryListener;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.Transaction;
import com.cloudata.btree.operation.ComplexOperation;
import com.cloudata.structured.StructuredProtocol.StructuredAction;
import com.cloudata.structured.StructuredProtocol.StructuredActionResponse;
import com.cloudata.structured.StructuredProtocol.StructuredActionType;
import com.cloudata.structured.StructuredProtocol.StructuredResponseEntry;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class ScanOperation extends StructuredOperationBase implements ComplexOperation<StructuredActionResponse> {

    public ScanOperation(StructuredAction action) {
        super(action);

        Preconditions.checkState(action.getAction() == StructuredActionType.LIST_WITH_PREFIX);
        Preconditions.checkState(!action.getIfNotExists());
        Preconditions.checkState(!action.hasIfVersion());
        Preconditions.checkState(action.hasKeyspaceId());

        Preconditions.checkState(returnKeys || returnValues);
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void doAction(Btree btree, Transaction txn) {
        Keyspace keyspace = Keyspace.user(action.getKeyspaceId());
        ByteString keyPrefix = action.getKey();

        boolean stripKeyspace = true;
        BtreeQuery query = new BtreeQuery(btree, keyspace, stripKeyspace, keyPrefix);

        try (KeyValueResultset rs = query.buildCursor(txn)) {
            rs.walk(new EntryListener() {
                @Override
                public boolean found(ByteBuffer key, Value value) {
                    StructuredResponseEntry.Builder eb = response.addEntryBuilder();
                    if (returnKeys) {
                        eb.setKey(ByteString.copyFrom(key));
                    }
                    if (returnValues) {
                        eb.setValue(ByteString.copyFrom(value.asBytes()));
                    }
                    return true;
                }
            });
        }
    }

}
