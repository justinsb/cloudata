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
import com.cloudata.keyvalue.KeyValueProtocol;
import com.cloudata.keyvalue.KeyValueProtocol.CommandAction;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueRequest;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueResponse;
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
    public Iterator<ByteString> listKeysWithPrefix(int space, ByteString prefix) {
        KeyValueRequest.Builder b = buildCommand(CommandAction.LIST_KEYS_WITH_PREFIX, space, prefix, null);

        KeyValueResponse response = executeSync(b);

        return response.getKeyList().iterator();
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
    public Iterator<KeyValueEntry> listEntriesWithPrefix(final int space, ByteString prefix) {
        KeyValueRequest.Builder b = buildCommand(CommandAction.LIST_ENTRIES_WITH_PREFIX, space, prefix, null);

        KeyValueResponse response = executeSync(b);

        return Iterators.transform(response.getEntryList().iterator(),
                new Function<KeyValueProtocol.Entry, KeyValueEntry>() {

                    @Override
                    public KeyValueEntry apply(KeyValueProtocol.Entry entry) {
                        return new KeyValueEntry(entry.getKey(), entry.getValue());
                    }

                });
    }

    @Override
    public ByteString read(int space, ByteString key) throws IOException {
        KeyValueRequest.Builder b = buildCommand(CommandAction.GET, space, key, null);

        KeyValueResponse response = executeSync(b);

        if (response.hasValue()) {
            return response.getValue();
        }

        return null;
    }

    @Override
    public boolean delete(int space, ByteString key, Modifier... modifiers) throws IOException {
        KeyValueRequest.Builder b = buildCommand(CommandAction.DELETE, space, key, null, modifiers);

        KeyValueResponse response = executeSync(b);

        return response.getSuccess();
    }

    private KeyValueRequest.Builder buildCommand(CommandAction action, int space, ByteString key, ByteString value,
            Modifier... modifiers) {
        KeyValueRequest.Builder b = KeyValueRequest.newBuilder();
        b.setStoreId(storeId);
        b.setSpaceId(space);
        b.setAction(action);
        b.setKey(key);
        if (value != null) {
            b.setValue(value);
        }

        if (modifiers != null) {
            for (Modifier modifier : modifiers) {
                if (modifier == IfNotExists.INSTANCE) {
                    b.setIfNotExists(true);
                } else if (modifier instanceof IfVersion) {
                    IfVersion ifVersion = (IfVersion) modifier;

                    b.setIfValue((ByteString) ifVersion.version);
                } else {
                    throw new IllegalArgumentException();
                }
            }
        }

        return b;
    }

    @Override
    public boolean putSync(int space, ByteString key, ByteString value, Modifier... modifiers) {
        try {
            return putAsync(space, key, value, modifiers).get();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ListenableFuture<Boolean> putAsync(final int space, final ByteString key, final ByteString value,
            final Modifier... modifiers) {
        KeyValueRequest.Builder b = buildCommand(CommandAction.SET, space, key, value, modifiers);

        ListenableFuture<KeyValueResponse> responseFuture = client.execute(b.build());
        return Futures.transform(responseFuture, new Function<KeyValueResponse, Boolean>() {

            @Override
            public Boolean apply(KeyValueResponse response) {
                return response.getSuccess();
            }

        });
    }

    @Override
    public ListenableFuture<Integer> delete(int keyspaceId, List<ByteString> keys) {
        KeyValueRequest.Builder b = KeyValueRequest.newBuilder();
        b.setStoreId(storeId);
        b.setSpaceId(keyspaceId);
        b.setAction(CommandAction.DELETE);
        b.addAllKeys(keys);

        ListenableFuture<KeyValueResponse> responseFuture = client.execute(b.build());
        return Futures.transform(responseFuture, new Function<KeyValueResponse, Integer>() {
            @Override
            public Integer apply(KeyValueResponse response) {
                if (!response.hasRowCount()) {
                    return null;
                }
                return response.getRowCount();
            }

        });
    }

}
