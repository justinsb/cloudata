package com.cloudata.keyvalue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.Btree;
import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.Database;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.ReadOnlyTransaction;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.keyvalue.operation.KeyValueOperation;
import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class KeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(KeyValueStore.class);

    final Btree btree;

    public KeyValueStore(File dir, boolean uniqueKeys) throws IOException {
        File data = new File(dir, "data");
        Database db = Database.build(data);

        log.warn("Building new btree @{}", dir);

        this.btree = new Btree(db, uniqueKeys);
    }

    public void doAction(KeyValueOperation operation) throws IOException {
        if (operation.isReadOnly()) {
            try (ReadOnlyTransaction txn = btree.beginReadOnly()) {
                txn.doAction(btree, operation);
            }
        } else {
            try (WriteTransaction txn = btree.beginReadWrite()) {
                txn.doAction(btree, operation);
                txn.commit();
            }
        }
    }

    public Value get(final ByteBuffer key) {
        try (ReadOnlyTransaction txn = btree.beginReadOnly()) {
            return txn.get(btree, key);
        }
    }

    public BtreeQuery buildQuery(Keyspace keyspace, boolean stripKeyspace, ByteString keyPrefix) {
        return new BtreeQuery(btree, keyspace, stripKeyspace, keyPrefix);
    }

}
