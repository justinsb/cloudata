package com.cloudata.btree;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Btree {
    private static final Logger log = LoggerFactory.getLogger(Btree.class);

    final Database db;
    final boolean uniqueKeys;

    final Lock writeLock = new ReentrantLock();

    public Btree(Database db, boolean uniqueKeys) {
        this.db = db;
        this.uniqueKeys = uniqueKeys;
    }

    public WriteTransaction beginReadWrite() {
        writeLock.lock();
        WriteTransaction txn = db.transactionTracker.beginReadWriteTransaction(writeLock);
        return txn;
    }

    public ReadOnlyTransaction beginReadOnly() {
        ReadOnlyTransaction txn = db.transactionTracker.beginReadOnlyTransaction();
        return txn;
    }

    public boolean isUniqueKeys() {
        return uniqueKeys;
    }

    public Database getDb() {
        return db;
    }

}
