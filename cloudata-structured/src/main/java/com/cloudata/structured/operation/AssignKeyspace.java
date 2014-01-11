package com.cloudata.structured.operation;

import java.util.Map.Entry;
import java.util.Set;

import com.cloudata.btree.Btree;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.Transaction;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.btree.operation.ComplexOperation;
import com.cloudata.btree.operation.SimpleSetOperation;
import com.cloudata.structured.StructuredProtocol.StructuredAction;
import com.cloudata.structured.StructuredProtocol.StructuredActionResponse;
import com.cloudata.structured.StructuredProtocol.StructuredActionType;
import com.cloudata.structured.StructuredStore;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;

public class AssignKeyspace extends StructuredOperationBase implements ComplexOperation<StructuredActionResponse> {
    private final StructuredStore store;

    public AssignKeyspace(StructuredStore store, StructuredAction action) {
        super(action);

        Preconditions.checkState(action.getAction() == StructuredActionType.ASSIGN_KEYSPACE);
    }

    @Override
    public void doAction(Btree btree, Transaction transaction) {
        WriteTransaction txn = (WriteTransaction) transaction;

        ByteString keyspaceName = action.getValue();
        Keyspace keyspace = store.getKeyspaces().ensureKeyspace(txn, keyspaceName);

        ByteString qualifiedKey = keyspace.mapToKey(key);

        // Set the value
        txn.doAction(btree, new SimpleSetOperation(qualifiedKey, newValue));

        // Update the key dictionary
        JsonObject json = newValue.asJsonObject();
        Set<String> keys = Sets.newHashSet();

        for (Entry<String, JsonElement> entry : json.entrySet()) {
            keys.add(entry.getKey());
        }

        store.ensureKeys(txn, keyspace, keys);
    }
}
