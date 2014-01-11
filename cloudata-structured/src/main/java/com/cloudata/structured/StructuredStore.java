package com.cloudata.structured;

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
import com.cloudata.structured.operation.StructuredOperation;
import com.cloudata.values.Value;

public class StructuredStore {

    private static final Logger log = LoggerFactory.getLogger(StructuredStore.class);

    final Btree btree;

    public StructuredStore(File dir, boolean uniqueKeys) throws IOException {
        File data = new File(dir, "data");
        Database db = Database.build(data, null);

        log.warn("Building new btree @{}", dir);

        this.btree = new Btree(db, uniqueKeys);
    }

    public void doAction(StructuredOperation operation) throws IOException {
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

    public BtreeQuery buildQuery(Keyspace keyspace, boolean stripKeyspace) {
        return new BtreeQuery(btree, keyspace, stripKeyspace);
    }

    public Btree getBtree() {
        return btree;
    }

    public Keyspaces getKeyspaces() {
        return new Keyspaces(btree);
    }

    public Keys getKeys() {
        return new Keys(btree);
    }

}
