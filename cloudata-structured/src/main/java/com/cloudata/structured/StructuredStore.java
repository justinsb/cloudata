package com.cloudata.structured;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.Btree;
import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.EntryListener;
import com.cloudata.btree.MmapPageStore;
import com.cloudata.btree.PageStore;
import com.cloudata.btree.ReadOnlyTransaction;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.structured.operation.StructuredOperation;
import com.cloudata.values.Value;

public class StructuredStore {

    private static final Logger log = LoggerFactory.getLogger(StructuredStore.class);

    final Btree btree;

    public StructuredStore(File dir, boolean uniqueKeys) throws IOException {
        File data = new File(dir, "data");
        PageStore pageStore = MmapPageStore.build(data, uniqueKeys);

        log.warn("Building new btree @{}", dir);

        this.btree = new Btree(pageStore, uniqueKeys);
    }

    public void doAction(ByteBuffer key, StructuredOperation<?> operation) {
        try (WriteTransaction txn = btree.beginReadWrite()) {
            txn.doAction(btree, key, operation);
            txn.commit();
        }
    }

    static class GetEntryListener implements EntryListener {
        final ByteBuffer findKey;
        Value foundValue;

        public GetEntryListener(ByteBuffer findKey) {
            this.findKey = findKey;
        }

        @Override
        public boolean found(ByteBuffer key, Value value) {
            // log.debug("Found {}={}", ByteBuffers.toHex(key), ByteBuffers.toHex(value));
            if (key.equals(findKey)) {
                foundValue = value;
            }
            // No more
            return false;
        }
    };

    public Value get(final ByteBuffer key) {
        try (ReadOnlyTransaction txn = btree.beginReadOnly()) {
            GetEntryListener listener = new GetEntryListener(key);
            txn.walk(btree, key, listener);

            Value value = listener.foundValue;
            // log.debug("Value for {}: {}", key, value);

            if (value != null) {
                // Once the transaction goes away the values may be invalid
                value = value.clone();
            }
            // log.debug("Value for {}: {}", key, value);

            return value;
        }
    }

    public BtreeQuery buildQuery() {
        return new BtreeQuery(btree);
    }

}
