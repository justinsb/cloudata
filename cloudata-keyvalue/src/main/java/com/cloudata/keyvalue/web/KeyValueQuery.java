package com.cloudata.keyvalue.web;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.btree.Btree;
import com.cloudata.keyvalue.btree.EntryListener;
import com.cloudata.keyvalue.btree.ReadOnlyTransaction;

public class KeyValueQuery {

    private final Btree btree;
    private final ByteBuffer start;

    public KeyValueQuery(Btree btree) {
        this.btree = btree;
        this.start = null;
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
            txn.walk(btree, start, entryListener);
        }

        @Override
        public void close() {
            txn.close();
        }

    }

}
