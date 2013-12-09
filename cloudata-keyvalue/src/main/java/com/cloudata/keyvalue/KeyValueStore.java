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
import com.cloudata.keyvalue.btree.ReadWriteTransaction;

public class KeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(KeyValueStore.class);

    final Btree btree;

    public KeyValueStore(File dir) throws IOException {
        File data = new File(dir, "data");
        PageStore pageStore = MmapPageStore.build(data);
        this.btree = new Btree(pageStore);
    }

    public void put(ByteBuffer key, ByteBuffer value) {
        ReadWriteTransaction txn = btree.beginReadWrite();

        txn.insert(key, value);
        txn.commit();
    }

    static class GetEntryListener implements EntryListener {
        final ByteBuffer findKey;
        ByteBuffer foundValue;

        public GetEntryListener(ByteBuffer findKey) {
            this.findKey = findKey;
        }

        @Override
        public boolean found(ByteBuffer key, ByteBuffer value) {
            log.debug("Found {}={}", ByteBuffers.toHex(key), ByteBuffers.toHex(value));
            if (key.equals(findKey)) {
                foundValue = value;
            }
            // No more
            return false;
        }
    };

    public ByteBuffer get(final ByteBuffer key) {
        ReadOnlyTransaction txn = btree.beginReadOnly();

        GetEntryListener listener = new GetEntryListener(key);
        txn.walk(key, listener);

        ByteBuffer value = listener.foundValue;

        log.debug("Value for {}: {}", key, value);
        txn.done();

        return value;
    }

}
