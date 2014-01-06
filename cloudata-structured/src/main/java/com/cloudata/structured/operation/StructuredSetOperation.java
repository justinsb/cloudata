package com.cloudata.structured.operation;

import java.util.Map.Entry;
import java.util.Set;

import com.cloudata.btree.Btree;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.btree.operation.ComplexOperation;
import com.cloudata.btree.operation.SimpleSetOperation;
import com.cloudata.structured.StructuredProto.LogAction;
import com.cloudata.structured.StructuredProto.LogEntry;
import com.cloudata.structured.StructuredStore;
import com.cloudata.values.Value;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;

public class StructuredSetOperation implements StructuredOperation<Void>, ComplexOperation<Void> {

    protected final Value newValue;
    private final StructuredStore store;
    private final ByteString keyspaceName;
    private final ByteString key;

    public StructuredSetOperation(StructuredStore store, ByteString keyspaceName, ByteString key, Value newValue) {
        this.store = store;
        this.keyspaceName = keyspaceName;
        this.key = key;
        this.newValue = newValue;
    }

    @Override
    public Void getResult() {
        return null;
    }

    @Override
    public LogEntry.Builder serialize() {
        LogEntry.Builder b = LogEntry.newBuilder();
        b.setKeyspaceName(keyspaceName);
        b.setKey(key);
        b.setAction(LogAction.SET);
        b.setValue(ByteString.copyFrom(newValue.serialize()));
        return b;
    }

    @Override
    public void doAction(Btree btree, WriteTransaction txn) {
        Keyspace keyspace = store.ensureKeyspace(txn, keyspaceName);

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
