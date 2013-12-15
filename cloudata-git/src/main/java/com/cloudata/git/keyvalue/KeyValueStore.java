package com.cloudata.git.keyvalue;

import java.io.IOException;

import com.google.protobuf.ByteString;

public interface KeyValueStore {

    Iterable<ByteString> listKeysWithPrefix(ByteString prefix);

    ByteString read(ByteString key) throws IOException;

    boolean delete(ByteString key, Modifier... modifiers) throws IOException;

    boolean put(ByteString key, ByteString value, Modifier... modifiers) throws IOException;
}
