package com.cloudata.keyvalue.btree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Btree {
    private static final Logger log = LoggerFactory.getLogger(Btree.class);

    final PageStore pageStore;
    final boolean uniqueKeys;

    public Btree(PageStore pageStore, boolean uniqueKeys) {
        this.pageStore = pageStore;
        this.uniqueKeys = uniqueKeys;
    }

    public ReadWriteTransaction beginReadWrite() {
        ReadWriteTransaction txn = new ReadWriteTransaction(pageStore);
        return txn;
    }

    public ReadOnlyTransaction beginReadOnly() {
        ReadOnlyTransaction txn = new ReadOnlyTransaction(pageStore);
        return txn;
    }

    public boolean isUniqueKeys() {
        return uniqueKeys;
    }

}
