package com.cloudata.btree;

import java.nio.ByteBuffer;

import javax.ws.rs.core.MediaType;

import com.google.protobuf.ByteString;

public class BtreeQuery {

    private final Btree btree;
    private final ByteBuffer start;
    private MediaType format;
    private final Keyspace keyspace;
    final boolean stripKeyspace;

    public BtreeQuery(Btree btree, Keyspace keyspace, boolean stripKeyspace) {
        this.btree = btree;
        this.keyspace = keyspace;
        this.stripKeyspace = stripKeyspace;

        if (keyspace != null) {
            this.start = keyspace.mapToKey(ByteString.EMPTY).asReadOnlyByteBuffer();
        } else {
            this.start = null;
        }

    }

    public KeyValueResultset execute() {
        ReadOnlyTransaction txn = null;
        KeyValueResultset cursor = null;

        try {
            txn = btree.beginReadOnly();

            cursor = new KeyValueResultset(txn);
            txn = null;
        } finally {
            if (txn != null) {
                txn.close();
            }
        }

        return cursor;
    }

    public class KeyValueResultset implements AutoCloseable {
        private final ReadOnlyTransaction txn;

        public KeyValueResultset(ReadOnlyTransaction txn) {
            this.txn = txn;
        }

        public void walk(EntryListener entryListener) {
            if (keyspace != null) {
                entryListener = new KeyspaceFilter(keyspace, stripKeyspace, entryListener);
            }
            txn.walk(btree, start, entryListener);
        }

        @Override
        public void close() {
            txn.close();
        }

    }

    public void setFormat(MediaType format) {
        this.format = format;
    }

    public MediaType getFormat() {
        return format;
    }

}
