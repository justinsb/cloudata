package com.cloudata.keyvalue.btree;

import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PageStore {

    private static final Logger log = LoggerFactory.getLogger(PageStore.class);

    protected int currentTransactionPage;
    private int currentRootPage;

    public abstract Page fetchPage(Page parent, int pageNumber);

    /**
     * Writes the page to the PageStore (disk, usually)
     * 
     * @param page
     * @return the new page number
     */
    public abstract int writePage(Page page);

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

    public ReadWriteTransaction beginReadWriteTransaction(Lock writeLock) {
        synchronized (this) {
            log.info("Starting new read-write transaction with root page: {}", currentRootPage);
            return new ReadWriteTransaction(this, writeLock, currentRootPage);
        }
    }
}
