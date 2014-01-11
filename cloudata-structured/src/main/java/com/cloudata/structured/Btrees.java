package com.cloudata.structured;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.Btree;
import com.cloudata.btree.EntryListener;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.values.Value;

public class Btrees {

    private static final Logger log = LoggerFactory.getLogger(Btrees.class);

    static class FindMaxListener implements EntryListener {
        final Keyspace keyspace;

        public FindMaxListener(Keyspace keyspace) {
            this.keyspace = keyspace;
        }

        private ByteBuffer lastKey;

        public ByteBuffer getLastKey() {
            return lastKey;
        }

        @Override
        public boolean found(ByteBuffer key, Value value) {
            if (keyspace.contains(key)) {
                this.lastKey = key;
                return true;
            } else {
                return false;
            }
        }

    }

    public static ByteBuffer findLast(Btree btree, WriteTransaction txn, Keyspace keyspace) {
        log.warn("Find max nextId logic is stupid");
        FindMaxListener listener = new FindMaxListener(keyspace);
        txn.walk(btree, keyspace.getKeyspacePrefix().asReadOnlyByteBuffer(), listener);
        ByteBuffer lastKey = listener.getLastKey();
        if (lastKey != null) {
            assert keyspace.contains(lastKey);
        }
        return lastKey;
    }
}
