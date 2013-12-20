package com.cloudata.clients.keyvalue;

import java.io.IOException;
import java.util.Iterator;

import com.google.protobuf.ByteString;

public interface KeyValueStore {

    Iterator<ByteString> listKeysWithPrefix(ByteString prefix);

    Iterator<KeyValueEntry> listEntriesWithPrefix(ByteString prefix);

    ByteString read(ByteString key) throws IOException;

    boolean delete(ByteString key, Modifier... modifiers) throws IOException;

    boolean put(ByteString key, ByteString value, Modifier... modifiers) throws IOException;
}
