package com.cloudata.keyvalue.operation;

import java.nio.ByteBuffer;

import com.cloudata.btree.Btree;
import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.BtreeQuery.KeyValueResultset;
import com.cloudata.btree.EntryListener;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.Transaction;
import com.cloudata.btree.operation.ComplexOperation;
import com.cloudata.keyvalue.KeyValueProtocol.ActionResponse;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueAction;
import com.cloudata.keyvalue.KeyValueProtocol.ResponseEntry;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class ScanOperation implements KeyValueOperation, ComplexOperation<ActionResponse> {
    protected final KeyValueAction action;

    protected final ActionResponse.Builder response = ActionResponse.newBuilder();

    final boolean fetchValues;

    public ScanOperation(KeyValueAction entry) {
        this.action = entry;

        // TODO: We could put fetchValues (and fetchKeys?) into the KeyValueAction
        switch (entry.getAction()) {
        case LIST_ENTRIES_WITH_PREFIX:
            fetchValues = true;
            break;

        case LIST_KEYS_WITH_PREFIX:
            fetchValues = false;
            break;

        default:
            throw new IllegalArgumentException();
        }

        Preconditions.checkState(!entry.getIfNotExists());
        Preconditions.checkState(!entry.hasIfValue());
        Preconditions.checkState(entry.hasStoreId());
        Preconditions.checkState(entry.hasKeyspaceId());
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public final KeyValueAction serialize() {
        return action;
    }

    @Override
    public ActionResponse getResult() {
        return response.build();
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
                    ResponseEntry.Builder eb = response.addEntryBuilder();
                    eb.setKey(ByteString.copyFrom(key));
                    if (fetchValues) {
                        eb.setValue(ByteString.copyFrom(value.asBytes()));
                    }
                    return true;
                }
            });
        }
    }

    @Override
    public long getStoreId() {
        return action.getStoreId();
    }

}
