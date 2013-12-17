package com.cloudata.btree;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.values.Value;

public class KeyspaceFilter implements EntryListener {
    private static final Logger log = LoggerFactory.getLogger(KeyspaceFilter.class);

    private final Keyspace keyspace;
    private final EntryListener inner;

    public KeyspaceFilter(Keyspace keyspace, EntryListener inner) {
        this.keyspace = keyspace;
        this.inner = inner;
    }

    @Override
    public boolean found(ByteBuffer key, Value value) {
        log.warn("KeyspaceFilter is stupid");
        if (keyspace.contains(key)) {
            return inner.found(key, value);
        }
        return true;
    }
}
