package com.cloudata.keyvalue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.btree.Btree;
import com.cloudata.keyvalue.btree.ByteBuffers;
import com.cloudata.keyvalue.btree.EntryListener;
import com.cloudata.keyvalue.btree.MmapPageStore;
import com.cloudata.keyvalue.btree.PageStore;
import com.cloudata.keyvalue.btree.ReadOnlyTransaction;
import com.cloudata.keyvalue.btree.WriteTransaction;
import com.cloudata.keyvalue.btree.operation.KeyOperation;
import com.cloudata.keyvalue.web.KeyValueQuery;

public class KeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(KeyValueStore.class);

    final Btree btree;

    public KeyValueStore(File dir, boolean uniqueKeys) throws IOException {
        File data = new File(dir, "data");
        PageStore pageStore = MmapPageStore.build(data, uniqueKeys);

        log.warn("Building new btree @{}", dir);

        this.btree = new Btree(pageStore, uniqueKeys);
    }

    public Object doAction(ByteBuffer key, KeyOperation operation) {
        try (WriteTransaction txn = btree.beginReadWrite()) {
            Object ret = txn.doAction(btree, key, operation);
            txn.commit();
            return ret;
        }
    }

    static class GetEntryListener implements EntryListener {
        final ByteBuffer findKey;
        ByteBuffer foundValue;

        public GetEntryListener(ByteBuffer findKey) {
            this.findKey = findKey;
        }

        @Override
        public boolean found(ByteBuffer key, ByteBuffer value) {
            // log.debug("Found {}={}", ByteBuffers.toHex(key), ByteBuffers.toHex(value));
            if (key.equals(findKey)) {
                foundValue = value;
            }
            // No more
            return false;
        }
    };

    public ByteBuffer get(final ByteBuffer key) {
        try (ReadOnlyTransaction txn = btree.beginReadOnly()) {
            GetEntryListener listener = new GetEntryListener(key);
            txn.walk(btree, key, listener);

            ByteBuffer value = listener.foundValue;
            // log.debug("Value for {}: {}", key, value);

            if (value != null) {
                // Once the transaction goes away the values may be invalid
                value = ByteBuffers.clone(value);
            }
            // log.debug("Value for {}: {}", key, value);

            return value;
        }
    }

    public KeyValueQuery buildQuery() {
        return new KeyValueQuery(btree);
    }

}
