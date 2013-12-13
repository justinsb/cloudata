package com.cloudata.keyvalue.btree;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.freemap.SpaceMapEntry;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

public abstract class PageStore {

    private static final Logger log = LoggerFactory.getLogger(PageStore.class);

    protected int currentTransactionPage;
    private int currentRootPage;
    private long currentTransactionId;

    private final List<ReadOnlyTransaction> readTransactions = Lists.newArrayList();
    private WriteTransaction writeTransaction;

    static class CleanupQueueEntry {
        final WriteTransaction transaction;
        final SpaceMapEntry space;
        final long transactionId;

        public CleanupQueueEntry(WriteTransaction transaction, SpaceMapEntry space) {
            this.transaction = transaction;
            this.transactionId = transaction.getTransactionId();
            assert this.transactionId != 0;
            this.space = space;
        }
    }

    final Queue<CleanupQueueEntry> writeTransactionCleanupQueue;

    public static class PageRecord {
        public final Page page;
        public final SpaceMapEntry space;

        public PageRecord(Page page, SpaceMapEntry space) {
            this.page = page;
            this.space = space;
        }
    }

    protected PageStore() {
        writeTransactionCleanupQueue = Queues.newArrayDeque();
    }

    public abstract PageRecord fetchPage(Page parent, int pageNumber);

    /**
     * Writes the page to the PageStore (disk, usually)
     * 
     * @param page
     * @return the new page number
     */
    public abstract SpaceMapEntry writePage(Page page);

    public abstract SpaceMapEntry commitTransaction(TransactionPage transaction);

    protected long nextTransactionId;

    public long assignTransactionId() {
        return nextTransactionId++;
    }

    public ReadOnlyTransaction beginReadOnlyTransaction() {
        synchronized (this) {
            log.info("Starting new read-only transaction with root page: {}", currentRootPage);
            ReadOnlyTransaction txn = new ReadOnlyTransaction(this, currentRootPage, currentTransactionId);
            readTransactions.add(txn);
            return txn;
        }
    }

    protected void setCurrent(int rootPage, long transactionId, int transactionPageId) {
        synchronized (this) {
            this.currentRootPage = rootPage;
            this.currentTransactionId = transactionId;
            this.currentTransactionPage = rootPage;
        }
    }

    public WriteTransaction beginReadWriteTransaction(Lock writeLock) {
        synchronized (this) {
            if (writeTransaction != null) {
                throw new IllegalStateException();
            }

            log.info("Starting new read-write transaction with root page: {}", currentRootPage);
            writeTransaction = new WriteTransaction(this, writeLock, currentRootPage);
            return writeTransaction;
        }
    }

    public void finished(Transaction transaction) {
        synchronized (this) {
            boolean wasRead = false;

            if (transaction == this.writeTransaction) {
                if (writeTransaction.getTransactionId() != 0) {
                    writeTransactionCleanupQueue.add(new CleanupQueueEntry(writeTransaction, writeTransaction
                            .getTransactionSpaceMapEntry()));
                } else {
                    // Transaction rollback...
                    if (!writeTransaction.getAllocated().isEmpty()) {
                        throw new UnsupportedOperationException();
                    }
                }
                this.writeTransaction = null;
            } else {
                if (!readTransactions.remove(transaction)) {
                    throw new IllegalStateException();
                }
                wasRead = true;
            }

            if (!writeTransactionCleanupQueue.isEmpty()) {
                long keepTransaction = Long.MAX_VALUE;
                for (ReadOnlyTransaction readTransaction : readTransactions) {
                    keepTransaction = Math.min(readTransaction.getSnapshotTransactionId(), keepTransaction);
                }

                while (!writeTransactionCleanupQueue.isEmpty()) {
                    CleanupQueueEntry cleanupQueueEntry = writeTransactionCleanupQueue.peek();
                    if (cleanupQueueEntry.transactionId >= keepTransaction) {
                        break;
                    }

                    cleanupQueueEntry = writeTransactionCleanupQueue.remove();
                    reclaim(cleanupQueueEntry.transaction, cleanupQueueEntry.space);
                }
            }

        }
    }

    protected abstract void reclaim(WriteTransaction cleanup, SpaceMapEntry txnSpace);
}
