package com.cloudata.btree;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.values.Value;

public class KeyspaceFilter implements EntryListener {
    private static final Logger log = LoggerFactory.getLogger(KeyspaceFilter.class);

    private final Keyspace keyspace;
    private final EntryListener inner;

    private final boolean stripKeyspace;

    public KeyspaceFilter(Keyspace keyspace, boolean stripKeyspace, EntryListener inner) {
        this.keyspace = keyspace;
        this.stripKeyspace = stripKeyspace;
        this.inner = inner;
    }

    @Override
    public boolean found(ByteBuffer key, Value value) {
        if (keyspace.contains(key)) {
            if (stripKeyspace) {
                key = keyspace.getSuffix(key);
            }
            return inner.found(key, value);
        }
        return false;
    }
}
