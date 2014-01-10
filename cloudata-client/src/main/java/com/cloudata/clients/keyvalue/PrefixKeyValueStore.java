package com.cloudata.clients.keyvalue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

public class PrefixKeyValueStore implements KeyValueStore {
    final KeyValueStore inner;
    final ByteString prefix;

    private PrefixKeyValueStore(KeyValueStore inner, ByteString prefix) {
        this.inner = inner;
        this.prefix = prefix;
    }

    public static PrefixKeyValueStore create(KeyValueStore inner, ByteString prefix) {
        if (inner instanceof PrefixKeyValueStore) {
            PrefixKeyValueStore innerPrefixKeyValueStore = (PrefixKeyValueStore) inner;
            ByteString combinedPrefix = innerPrefixKeyValueStore.prefix.concat(prefix);
            return PrefixKeyValueStore.create(innerPrefixKeyValueStore.inner, combinedPrefix);
        } else {
            return new PrefixKeyValueStore(inner, prefix);
        }
    }

    @Override
    public Iterator<ByteString> listKeysWithPrefix(int space, ByteString filter) {
        ByteString filterWithPrefix = this.prefix.concat(filter);
        Iterator<ByteString> keys = inner.listKeysWithPrefix(space, filterWithPrefix);

        return Iterators.transform(keys, new Function<ByteString, ByteString>() {

            @Override
            public ByteString apply(ByteString withPrefix) {
                assert withPrefix.substring(0, prefix.size()) == prefix;

                return withPrefix.substring(prefix.size());
            }

        });
    }

    @Override
    public Iterator<KeyValueEntry> listEntriesWithPrefix(int space, ByteString filter) {
        ByteString filterWithPrefix = this.prefix.concat(filter);
        Iterator<KeyValueEntry> entries = inner.listEntriesWithPrefix(space, filterWithPrefix);

        return Iterators.transform(entries, new Function<KeyValueEntry, KeyValueEntry>() {

            @Override
            public KeyValueEntry apply(KeyValueEntry entry) {
                ByteString key = entry.getKey();

                assert key.substring(0, prefix.size()) == prefix;

                return new KeyValueEntry(key.substring(prefix.size()), entry.getValue());
            }

        });

    }

    @Override
    public ByteString read(int space, ByteString key) throws IOException {
        ByteString keyWithPrefix = prefix.concat(key);
        return inner.read(space, keyWithPrefix);
    }

    @Override
    public boolean delete(int space, ByteString key, Modifier... modifiers) throws IOException {
        ByteString keyWithPrefix = prefix.concat(key);
        return inner.delete(space, keyWithPrefix, modifiers);
    }

    @Override
    public boolean putSync(int space, ByteString key, ByteString value, Modifier... modifiers) throws IOException {
        ByteString keyWithPrefix = prefix.concat(key);
        return inner.putSync(space, keyWithPrefix, value, modifiers);
    }

    @Override
    public ListenableFuture<Boolean> putAsync(int space, ByteString key, ByteString value, Modifier... modifiers) {
        ByteString keyWithPrefix = prefix.concat(key);
        return inner.putAsync(space, keyWithPrefix, value, modifiers);
    }

    @Override
    public ListenableFuture<Integer> delete(int keyspaceId, List<ByteString> keys) {
        List<ByteString> keysWithPrefix = Lists.newArrayList();
        for (ByteString key : keys) {
            keysWithPrefix.add(prefix.concat(key));
        }
        return inner.delete(keyspaceId, keysWithPrefix);
    }

}
