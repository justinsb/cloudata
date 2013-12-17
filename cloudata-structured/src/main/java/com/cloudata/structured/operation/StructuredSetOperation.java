package com.cloudata.structured.operation;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.Set;

import com.cloudata.btree.Btree;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.btree.operation.ComplexOperation;
import com.cloudata.btree.operation.SetOperation;
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

    public StructuredSetOperation(StructuredStore store, Value newValue) {
        this.store = store;
        this.newValue = newValue;
    }

    @Override
    public Void getResult() {
        return null;
    }

    @Override
    public LogEntry.Builder serialize() {
        LogEntry.Builder b = LogEntry.newBuilder();
        b.setAction(LogAction.SET);
        b.setValue(ByteString.copyFrom(newValue.serialize()));
        return b;
    }

    @Override
    public void doAction(Btree btree, WriteTransaction txn, ByteBuffer key) {
        // Set the value
        txn.doAction(btree, key, new SetOperation(newValue));

        // Update the key dictionary
        JsonObject json = newValue.asJsonObject();
        Set<String> keys = Sets.newHashSet();

        for (Entry<String, JsonElement> entry : json.entrySet()) {
            keys.add(entry.getKey());
        }

        store.ensureKeys(txn, keys);
    }

}
