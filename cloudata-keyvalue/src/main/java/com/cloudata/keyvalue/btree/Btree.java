package com.cloudata.keyvalue.btree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Btree {
    private static final Logger log = LoggerFactory.getLogger(Btree.class);

    final PageStore pageStore;

    public Btree(PageStore pageStore) {
        this.pageStore = pageStore;
    }

    public ReadWriteTransaction beginReadWrite() {
        ReadWriteTransaction txn = new ReadWriteTransaction(pageStore);
        return txn;
    }

    public ReadOnlyTransaction beginReadOnly() {
        ReadOnlyTransaction txn = new ReadOnlyTransaction(pageStore);
        return txn;
    }

}
