package com.cloudata.keyvalue.btree;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Btree {
    private static final Logger log = LoggerFactory.getLogger(Btree.class);

    final PageStore pageStore;
    final boolean uniqueKeys;

    final Lock writeLock = new ReentrantLock();

    public Btree(PageStore pageStore, boolean uniqueKeys) {
        this.pageStore = pageStore;
        this.uniqueKeys = uniqueKeys;
    }

    public WriteTransaction beginReadWrite() {
        writeLock.lock();
        WriteTransaction txn = pageStore.beginReadWriteTransaction(writeLock);
        return txn;
    }

    public ReadOnlyTransaction beginReadOnly() {
        ReadOnlyTransaction txn = pageStore.beginReadOnlyTransaction();
        return txn;
    }

    public boolean isUniqueKeys() {
        return uniqueKeys;
    }

}
