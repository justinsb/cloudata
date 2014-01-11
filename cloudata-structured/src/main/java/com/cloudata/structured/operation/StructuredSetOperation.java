package com.cloudata.structured.operation;

import java.util.Map.Entry;
import java.util.Set;

import com.cloudata.btree.Btree;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.Transaction;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.btree.operation.ComplexOperation;
import com.cloudata.btree.operation.SimpleSetOperation;
import com.cloudata.clients.structured.AlreadyExistsException;
import com.cloudata.structured.Keyspaces;
import com.cloudata.structured.StructuredProtocol.ActionResponseCode;
import com.cloudata.structured.StructuredProtocol.KeyspaceData;
import com.cloudata.structured.StructuredProtocol.StructuredAction;
import com.cloudata.structured.StructuredProtocol.StructuredActionResponse;
import com.cloudata.structured.StructuredProtocol.StructuredActionType;
import com.cloudata.structured.StructuredProtocol.StructuredResponseEntry;
import com.cloudata.structured.StructuredStore;
import com.cloudata.structured.SystemKeyspaces;
import com.cloudata.values.Value;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class StructuredSetOperation extends StructuredOperationBase implements
        ComplexOperation<StructuredActionResponse> {

    private final StructuredStore store;

    public StructuredSetOperation(StructuredAction action, StructuredStore store) {
        super(action);
        this.store = store;
    }

    @Override
    public void doAction(Btree btree, Transaction transaction) {
        WriteTransaction txn = (WriteTransaction) transaction;

        if (keyspace.isSystem()) {
            doSystemAction(btree, txn);
            return;
        }

        // TODO: Get tablespace, check the type there (protobuf vs json etc)

        // Set the value
        Value newValue = Value.fromRawBytes(action.getValue());
        txn.doAction(btree, new SimpleSetOperation(qualifiedKey, newValue));

        if (isJsonKeyspace(keyspace)) {
            // Update the key dictionary
            JsonObject json = newValue.asJsonObject();
            Set<String> keys = Sets.newHashSet();

            for (Entry<String, JsonElement> entry : json.entrySet()) {
                keys.add(entry.getKey());
            }

            Keyspace keyspace = Keyspace.user(action.getKeyspaceId());
            store.getKeys().ensureKeys(txn, keyspace, Iterables.transform(keys, new Function<String, ByteString>() {
                @Override
                public ByteString apply(String input) {
                    return ByteString.copyFromUtf8(input);
                }
            }));
        }

        // Return DONE
        StructuredResponseEntry.Builder eb = response.addEntryBuilder();
        if (returnKeys) {
            eb.setKey(action.getKey());
        }
        if (returnValues) {
            eb.setValue(action.getValue());
        }
        eb.setCode(ActionResponseCode.DONE);
    }

    private boolean isJsonKeyspace(Keyspace keyspace) {
        // TODO: Figure out how to cope with JSON keyspaces
        return false;
    }

    public static StructuredSetOperation build(long storeId, Keyspace keyspace, ByteString key, Value value) {
        StructuredAction.Builder b = StructuredAction.newBuilder();
        b.setAction(StructuredActionType.STRUCTURED_SET);
        b.setStoreId(storeId);
        b.setKeyspaceId(keyspace.getKeyspaceId());
        b.setKey(key);
        b.setValue(ByteString.copyFrom(value.asBytes()));

        // We don't need the store yet..
        StructuredStore store = null;

        return new StructuredSetOperation(b.build(), store);
    }

    private void doSystemAction(Btree btree, WriteTransaction txn) {
        ByteString value = null;

        StructuredResponseEntry.Builder eb = response.addEntryBuilder();
        if (returnKeys) {
            eb.setKey(action.getKey());
        }

        try {
            switch (keyspace.getKeyspaceId()) {
            case SystemKeyspaces.KEYSPACE_DEFINITIONS:
                value = doKeyspaceDefinitionAction(btree, txn);
                break;

            default:
                throw new IllegalArgumentException();
            }
            eb.setCode(ActionResponseCode.DONE);
        } catch (AlreadyExistsException e) {
            eb.setCode(ActionResponseCode.ALREADY_EXISTS);
        }

        if (returnValues && value != null) {
            eb.setValue(value);
        }
    }

    private ByteString doKeyspaceDefinitionAction(Btree btree, WriteTransaction txn) throws AlreadyExistsException {
        try {
            KeyspaceData keyspaceData = KeyspaceData.parseFrom(action.getValue());
            if (action.hasKey()) {
                throw new IllegalArgumentException();
            }

            Keyspaces keyspaces = new Keyspaces(btree);
            keyspaceData = keyspaces.createKeyspace(txn, keyspaceData);

            return keyspaceData.toByteString();
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException();
        }
    }

}
