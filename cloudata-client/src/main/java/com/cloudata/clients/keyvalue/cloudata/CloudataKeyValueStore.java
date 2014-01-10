package com.cloudata.clients.keyvalue.cloudata;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.clients.keyvalue.IfNotExists;
import com.cloudata.clients.keyvalue.IfVersion;
import com.cloudata.clients.keyvalue.KeyValueEntry;
import com.cloudata.clients.keyvalue.KeyValueStore;
import com.cloudata.clients.keyvalue.Modifier;
import com.cloudata.keyvalue.KeyValueProtocol.ActionResponse;
import com.cloudata.keyvalue.KeyValueProtocol.ActionType;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueAction;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueAction.Builder;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueRequest;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueResponse;
import com.cloudata.keyvalue.KeyValueProtocol.ResponseEntry;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

public class CloudataKeyValueStore implements KeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(CloudataKeyValueStore.class);

    final CloudataKeyValueClient client;
    final long storeId;

    public CloudataKeyValueStore(CloudataKeyValueClient client, long storeId) {
        this.client = client;
        this.storeId = storeId;
    }

    @Override
    public Iterator<ByteString> listKeysWithPrefix(int keyspaceId, ByteString prefix) {
        KeyValueRequest.Builder b = buildCommand(ActionType.LIST_KEYS_WITH_PREFIX, keyspaceId, prefix, null);

        KeyValueResponse response = executeSync(b);

        return Iterators.transform(response.getActionResponse().getEntryList().iterator(),
                new Function<ResponseEntry, ByteString>() {

                    @Override
                    public ByteString apply(ResponseEntry entry) {
                        return entry.getKey();
                    }

                });
    }

    private KeyValueResponse executeSync(KeyValueRequest.Builder b) {
        ListenableFuture<KeyValueResponse> responseFuture = client.execute(b.build());
        KeyValueResponse response;
        try {
            response = responseFuture.get();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        return response;
    }

    @Override
    public Iterator<KeyValueEntry> listEntriesWithPrefix(final int keyspaceId, ByteString prefix) {
        KeyValueRequest.Builder b = buildCommand(ActionType.LIST_ENTRIES_WITH_PREFIX, keyspaceId, prefix, null);

        KeyValueResponse response = executeSync(b);

        return Iterators.transform(response.getActionResponse().getEntryList().iterator(),
                new Function<ResponseEntry, KeyValueEntry>() {

                    @Override
                    public KeyValueEntry apply(ResponseEntry entry) {
                        return new KeyValueEntry(entry.getKey(), entry.getValue());
                    }

                });
    }

    @Override
    public ByteString read(int keyspaceId, ByteString key) throws IOException {
        KeyValueRequest.Builder b = buildCommand(ActionType.GET, keyspaceId, key, null);

        KeyValueResponse response = executeSync(b);

        ActionResponse actionResponse = response.getActionResponse();
        if (actionResponse.getEntryCount() != 0) {
            assert actionResponse.getEntryCount() == 1;

            ResponseEntry entry = actionResponse.getEntry(0);
            assert entry.hasValue();
            return entry.getValue();
        }

        return null;
    }

    @Override
    public boolean delete(int keyspaceId, ByteString key, Modifier... modifiers) throws IOException {
        KeyValueRequest.Builder b = buildCommand(ActionType.DELETE, keyspaceId, key, null, modifiers);

        KeyValueResponse response = executeSync(b);
        int changes = countChanges(response.getActionResponse());
        return changes != 0;
    }

    private KeyValueRequest.Builder buildCommand(ActionType action, int keyspaceId, ByteString key, ByteString value,
            Modifier... modifiers) {
        KeyValueRequest.Builder b = KeyValueRequest.newBuilder();
        Builder ab = b.getActionBuilder();

        ab.setStoreId(storeId);
        ab.setKeyspaceId(keyspaceId);
        ab.setKey(key);
        ab.setAction(action);
        if (value != null) {
            ab.setValue(value);
        }

        if (modifiers != null) {
            for (Modifier modifier : modifiers) {
                if (modifier == IfNotExists.INSTANCE) {
                    ab.setIfNotExists(true);
                } else if (modifier instanceof IfVersion) {
                    IfVersion ifVersion = (IfVersion) modifier;

                    ab.setIfValue((ByteString) ifVersion.version);
                } else {
                    throw new IllegalArgumentException();
                }
            }
        }

        return b;
    }

    @Override
    public boolean putSync(int keyspaceId, ByteString key, ByteString value, Modifier... modifiers) {
        try {
            return putAsync(keyspaceId, key, value, modifiers).get();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ListenableFuture<Boolean> putAsync(final int keyspaceId, final ByteString key, final ByteString value,
            final Modifier... modifiers) {
        KeyValueRequest.Builder b = buildCommand(ActionType.SET, keyspaceId, key, value, modifiers);

        ListenableFuture<KeyValueResponse> responseFuture = client.execute(b.build());
        return Futures.transform(responseFuture, new Function<KeyValueResponse, Boolean>() {

            @Override
            public Boolean apply(KeyValueResponse response) {
                int changes = countChanges(response.getActionResponse());
                return changes != 0;
            }

        });
    }

    @Override
    public ListenableFuture<Integer> delete(int keyspaceId, List<ByteString> keys) {
        KeyValueRequest.Builder b = KeyValueRequest.newBuilder();

        KeyValueAction.Builder cb = b.getActionBuilder();
        cb.setStoreId(storeId);
        cb.setKeyspaceId(keyspaceId);
        cb.setAction(ActionType.COMPOUND);

        for (ByteString key : keys) {
            KeyValueAction.Builder ab = cb.addChildrenBuilder();
            ab.setStoreId(storeId);
            ab.setKeyspaceId(keyspaceId);
            ab.setAction(ActionType.DELETE);
            ab.setKey(key);
        }

        ListenableFuture<KeyValueResponse> responseFuture = client.execute(b.build());
        return Futures.transform(responseFuture, new Function<KeyValueResponse, Integer>() {
            @Override
            public Integer apply(KeyValueResponse response) {
                int changes = countChanges(response.getActionResponse());
                return changes;
            }

        });
    }

    private int countChanges(ActionResponse response) {
        int changed = 0;

        if (response.getChildrenCount() != 0) {
            for (ActionResponse child : response.getChildrenList()) {
                changed += countChanges(child);
            }
        }

        if (response.getEntryCount() != 0) {
            for (ResponseEntry entry : response.getEntryList()) {
                if (entry.getChanged()) {
                    changed++;
                }
            }
        }

        return changed;
    }
}
