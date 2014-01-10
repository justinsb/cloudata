package com.cloudata.clients.structured;

import java.io.IOException;
import java.util.Iterator;

import com.google.protobuf.ByteString;

public interface StructuredStore {
    // Iterator<ByteString> listKeysWithPrefix(int space, ByteString prefix);
    //
    // Iterator<KeyValueEntry> listEntriesWithPrefix(int space, ByteString prefix);
    //
    // ByteString read(int space, ByteString key) throws IOException;
    //
    // boolean delete(int space, ByteString key, Modifier... modifiers) throws IOException;
    //
    // ListenableFuture<Boolean> putAsync(int space, ByteString key, ByteString value, Modifier... modifiers);
    //
    // boolean putSync(int space, ByteString key, ByteString value, Modifier... modifiers) throws IOException;
    //
    // ListenableFuture<Integer> delete(int keyspaceId, List<ByteString> keys);

    public interface Entry {

        ByteString getData();

        int getVersion();

    }

    public static final int SYSTEM_TABLESPACE_START = 2100000000;
    public static final int TABLESPACEID_TABLESPACES = SYSTEM_TABLESPACE_START + 0;
    public static final int TABLESPACEID_TABLESPACE_ID_INDEX = SYSTEM_TABLESPACE_START + 1;

    Entry read(int tablespace, ByteString key, ReadOption... options) throws IOException;

    boolean delete(int tablespaceId, ByteString cloudataKey) throws IOException;

    boolean delete(int tablespaceId, ByteString cloudataKey, int ifVersion) throws IOException,
            VersionMismatchException;

    Entry create(int tablespaceId, ByteString cloudataKey, ByteString data) throws IOException, AlreadyExistsException;

    Entry put(int tablespaceId, ByteString cloudataKey, ByteString data, int readVersion) throws IOException,
            VersionMismatchException;

    Iterator<ByteString> listKeysWithPrefix(int tablespaceId, ByteString filterBytes) throws IOException;

    StructuredStoreSchema getSchema();
}
