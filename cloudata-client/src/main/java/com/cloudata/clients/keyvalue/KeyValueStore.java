package com.cloudata.clients.keyvalue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

public interface KeyValueStore {
    Iterator<ByteString> listKeysWithPrefix(int space, ByteString prefix);

    Iterator<KeyValueEntry> listEntriesWithPrefix(int space, ByteString prefix);

    ByteString read(int space, ByteString key) throws IOException;

    boolean delete(int space, ByteString key, Modifier... modifiers) throws IOException;

    ListenableFuture<Boolean> putAsync(int space, ByteString key, ByteString value, Modifier... modifiers);

    boolean putSync(int space, ByteString key, ByteString value, Modifier... modifiers) throws IOException;

    ListenableFuture<Integer> delete(int keyspaceId, List<ByteString> keys);
}
