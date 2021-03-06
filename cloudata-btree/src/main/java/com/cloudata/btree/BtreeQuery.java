package com.cloudata.btree;

import java.nio.ByteBuffer;

import javax.ws.rs.core.MediaType;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class BtreeQuery {

    private final Btree btree;
    private final ByteBuffer start;
    private MediaType format;
    private final Keyspace keyspace;
    final boolean stripKeyspace;
    private final ByteString prefix;

    public BtreeQuery(Btree btree, Keyspace keyspace, boolean stripKeyspace) {
        this(btree, keyspace, stripKeyspace, null);
    }

    public BtreeQuery(Btree btree, Keyspace keyspace, boolean stripKeyspace, ByteString prefix) {
        this.btree = btree;
        this.keyspace = keyspace;
        this.stripKeyspace = stripKeyspace;
        this.prefix = prefix;

        if (keyspace != null) {
            if (prefix == null) {
                this.start = keyspace.mapToKey(ByteString.EMPTY).asReadOnlyByteBuffer();
            } else {
                this.start = keyspace.mapToKey(prefix).asReadOnlyByteBuffer();
            }
        } else {
            Preconditions.checkArgument(!stripKeyspace);
            Preconditions.checkArgument(prefix == null);
            this.start = null;
        }

    }

    public KeyValueResultset buildCursor() {
        ReadOnlyTransaction txn = null;
        KeyValueResultset cursor = null;

        try {
            txn = btree.beginReadOnly();

            cursor = new KeyValueResultset(txn, true);
            txn = null;
        } finally {
            if (txn != null) {
                txn.close();
            }
        }

        return cursor;
    }

    public KeyValueResultset buildCursor(Transaction txn) {
        return new KeyValueResultset(txn, false);
    }

    public class KeyValueResultset implements AutoCloseable {
        private final Transaction txn;
        private final boolean closeTransaction;

        public KeyValueResultset(Transaction txn, boolean closeTransaction) {
            this.txn = txn;
            this.closeTransaction = closeTransaction;
        }

        public void walk(EntryListener entryListener) {
            if (keyspace != null || prefix != null) {
                entryListener = new KeyspaceFilter(keyspace, stripKeyspace, prefix, entryListener);
            }
            txn.walk(btree, start, entryListener);
        }

        @Override
        public void close() {
            if (closeTransaction) {
                txn.close();
            }
        }

    }

    public void setFormat(MediaType format) {
        this.format = format;
    }

    public MediaType getFormat() {
        return format;
    }

}
