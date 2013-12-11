package com.cloudata.keyvalue.btree;

import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.freemap.SpaceMapEntry;

public abstract class PageStore {

    private static final Logger log = LoggerFactory.getLogger(PageStore.class);

    protected int currentTransactionPage;
    private int currentRootPage;

    public static class PageRecord {
        public final Page page;
        public final SpaceMapEntry space;

        public PageRecord(Page page, SpaceMapEntry space) {
            this.page = page;
            this.space = space;
        }

    }

    public abstract PageRecord fetchPage(Page parent, int pageNumber);

    /**
     * Writes the page to the PageStore (disk, usually)
     * 
     * @param page
     * @return the new page number
     */
    public abstract SpaceMapEntry writePage(Page page);

    public abstract void commitTransaction(TransactionPage transaction);

    protected long nextTransactionId;

    public long assignTransactionId() {
        return nextTransactionId++;
    }

    public ReadOnlyTransaction beginReadOnlyTransaction() {
        synchronized (this) {
            log.info("Starting new read-write transaction with root page: {}", currentRootPage);
            return new ReadOnlyTransaction(this, currentRootPage);
        }
    }

    protected void setCurrent(int rootPage, int transactionPageId) {
        synchronized (this) {
            this.currentRootPage = rootPage;
            this.currentTransactionPage = rootPage;
        }
    }

    public WriteTransaction beginReadWriteTransaction(Lock writeLock) {
        synchronized (this) {
            log.info("Starting new read-write transaction with root page: {}", currentRootPage);
            return new WriteTransaction(this, writeLock, currentRootPage);
        }
    }
}
