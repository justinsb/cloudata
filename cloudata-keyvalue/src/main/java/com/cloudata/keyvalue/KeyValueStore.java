package com.cloudata.keyvalue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.Btree;
import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.MmapPageStore;
import com.cloudata.btree.PageStore;
import com.cloudata.btree.ReadOnlyTransaction;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.keyvalue.operation.KeyOperation;
import com.cloudata.values.Value;

public class KeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(KeyValueStore.class);

    final Btree btree;

    public KeyValueStore(File dir, boolean uniqueKeys) throws IOException {
        File data = new File(dir, "data");
        PageStore pageStore = MmapPageStore.build(data, uniqueKeys);

        log.warn("Building new btree @{}", dir);

        this.btree = new Btree(pageStore, uniqueKeys);
    }

    public void doAction(KeyOperation<?> operation) {
        try (WriteTransaction txn = btree.beginReadWrite()) {
            txn.doAction(btree, operation);
            txn.commit();
        }
    }

    public Value get(final ByteBuffer key) {
        try (ReadOnlyTransaction txn = btree.beginReadOnly()) {
            return txn.get(btree, key);
        }
    }

    public BtreeQuery buildQuery(Keyspace keyspace, boolean stripKeyspace) {
        return new BtreeQuery(btree, keyspace, stripKeyspace);
    }

}
