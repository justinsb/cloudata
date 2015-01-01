package com.cloudata.btree;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class KeyspaceFilter implements EntryListener {
    private static final Logger log = LoggerFactory.getLogger(KeyspaceFilter.class);

    private final Keyspace keyspace;
    private final EntryListener inner;

    private final boolean stripKeyspace;

    private final ByteString prefix;

    private final ByteString prefixMatch;

    public KeyspaceFilter(Keyspace keyspace, boolean stripKeyspace, ByteString prefix, EntryListener inner) {
        this.keyspace = keyspace;
        this.stripKeyspace = stripKeyspace;
        this.prefix = prefix;
        this.inner = inner;

        Preconditions.checkArgument(keyspace != null);

        if (prefix == null) {
            this.prefixMatch = keyspace.mapToKey(ByteString.EMPTY);
        } else {
            this.prefixMatch = keyspace.mapToKey(prefix);
        }
    }

    @Override
    public boolean found(ByteBuffer key, Value value) {
        if (ByteBuffers.startsWith(key, prefixMatch)) {
            if (stripKeyspace) {
                key = keyspace.getSuffix(key);
            }
            return inner.found(key, value);
        }
        return false;
    }
}
