package com.cloudata.keyvalue.btree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadOnlyTransaction extends Transaction {
    private static final Logger log = LoggerFactory.getLogger(ReadOnlyTransaction.class);

    public ReadOnlyTransaction(PageStore pageStore) {
        super(pageStore);
    }

    public void done() {

    }

    @Override
    public Page getPage(Page parent, int pageNumber) {
        // TODO: Should we have a small cache?
        return pageStore.fetchPage(parent, pageNumber);
    }

    int rootPageId;

    @Override
    protected Page getRootPage(boolean create) {
        if (rootPageId == 0) {
            rootPageId = pageStore.getRootPageId();
        }

        if (rootPageId == 0) {
            if (!create) {
                return null;
            }
            throw new IllegalStateException();
        }

        return getPage(null, rootPageId);
    }
}
