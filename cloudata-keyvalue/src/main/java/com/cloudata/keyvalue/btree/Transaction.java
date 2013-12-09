package com.cloudata.keyvalue.btree;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Transaction {
    private static final Logger log = LoggerFactory.getLogger(Transaction.class);

    final PageStore pageStore;

    public Transaction(PageStore pageStore) {
        this.pageStore = pageStore;
    }

    public abstract Page getPage(Page parent, int pageNumber);

    public void walk(ByteBuffer from, EntryListener listener) {
        Page rootPage = getRootPage(false);
        if (rootPage == null) {
            log.info("No data; returning immediately from walk");
            return;
        }
        rootPage.walk(this, from, listener);
    }

    protected abstract Page getRootPage(boolean create);
}
