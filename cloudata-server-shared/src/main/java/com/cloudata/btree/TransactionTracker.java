package com.cloudata.btree;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.freemap.FreeSpaceMap;
import com.cloudata.freemap.FreeSpaceMap.SnapshotWritingPage;
import com.cloudata.freemap.SpaceMapEntry;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;

public class TransactionTracker {

    private static final Logger log = LoggerFactory.getLogger(TransactionTracker.class);

    protected int currentTransactionPageId;
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
    final Database db;

    final FreeSpaceMap freeSpaceMap;
    final List<SpaceMapEntry> deferredReclaim;

    long nextTransactionId;

    long assignTransactionId() {
        return nextTransactionId++;
    }

    TransactionTracker(Database db, MasterPage latest) throws IOException {
        this.db = db;
        this.writeTransactionCleanupQueue = Queues.newArrayDeque();

        this.deferredReclaim = Lists.newArrayList();

        this.freeSpaceMap = recoverFreeSpaceMap(db.pageStore, latest);

        this.nextTransactionId = latest.getTransactionId() + 1;
        setCurrent(latest.getRoot(), latest.getTransactionId(), latest.getTransactionPageId());
    }

    private static FreeSpaceMap recoverFreeSpaceMap(PageStore pageStore, MasterPage latest) throws IOException {
        // TODO: We could create a 'system transaction'
        List<PageRecord> freeList = Lists.newArrayList();

        try {
            int transactionPageId = latest.getTransactionPageId();
            List<PageRecord> history = Lists.newArrayList();

            PageRecord fsmSnapshot = null;

            // Walk the list of transactions backwards until we find a FSM snapshot
            if (transactionPageId != 0) {
                PageRecord current = getSystemPage(pageStore, transactionPageId);
                freeList.add(current);

                while (true) {
                    TransactionPage transactionPage = (TransactionPage) current.page;
                    history.add(current);
                    if (transactionPage.getFreeSpaceSnapshotId() != 0) {
                        fsmSnapshot = getSystemPage(pageStore, transactionPage.getFreeSpaceSnapshotId());
                        assert fsmSnapshot != null;
                        freeList.add(fsmSnapshot);
                        break;
                    }

                    int previousTransactionPageId = transactionPage.getPreviousTransactionPageId();
                    if (previousTransactionPageId == 0) {
                        break;
                    }
                    PageRecord previous = getSystemPage(pageStore, previousTransactionPageId);
                    assert (previous != null);
                    freeList.add(previous);
                    current = previous;
                }

                Collections.reverse(history);
            }

            FreeSpaceMap fsm;
            if (fsmSnapshot == null) {
                fsm = pageStore.createEmptyFreeSpaceMap();
            } else {
                fsm = FreeSpaceMap.createFromSnapshot(fsmSnapshot);
            }

            for (PageRecord txnRecord : history) {
                fsm.replay(txnRecord);
            }

            return fsm;
        } finally {
            for (PageRecord pr : freeList) {
                pr.release();
            }
        }
    }

    private static PageRecord getSystemPage(PageStore pageStore, int pageNumber) throws IOException {
        Btree btree = null;
        Page parent = null;
        return Futures.get(pageStore.fetchPage(btree, parent, pageNumber), IOException.class);
    }

    SpaceMapEntry allocate(int allocateSlots) {
        int allocated = freeSpaceMap.allocate(allocateSlots);
        if (allocated < 0) {
            // TODO: Grow database
            throw new IllegalStateException();
        }

        return new SpaceMapEntry(allocated, allocateSlots);
    }

    WriteTransaction beginReadWriteTransaction(Lock writeLock) {
        synchronized (this) {
            if (writeTransaction != null) {
                throw new IllegalStateException();
            }

            log.info("Starting new read-write transaction with root page: {}", currentRootPage);
            writeTransaction = new WriteTransaction(db, writeLock, currentRootPage);
            return writeTransaction;
        }
    }

    ReadOnlyTransaction beginReadOnlyTransaction() {
        synchronized (this) {
            log.info("Starting new read-only transaction with root page: {}", currentRootPage);
            ReadOnlyTransaction txn = new ReadOnlyTransaction(db, currentRootPage, currentTransactionId);
            readTransactions.add(txn);
            return txn;
        }
    }

    protected void setCurrent(int rootPage, long transactionId, int transactionPageId) {
        synchronized (this) {
            this.currentRootPage = rootPage;
            this.currentTransactionId = transactionId;
            this.currentTransactionPageId = transactionPageId;
        }
    }

    void finished(Transaction transaction) {
        synchronized (this) {
            boolean wasRead = false;

            if (transaction == this.writeTransaction) {
                if (writeTransaction.isCommitted()) {
                    writeTransactionCleanupQueue.add(new CleanupQueueEntry(writeTransaction, writeTransaction
                            .getTransactionSpaceMapEntry()));
                } else {
                    // Transaction rollback...
                    if (!writeTransaction.getAllocated().isEmpty()) {
                        throw new UnsupportedOperationException("Rollback is not yet supported");
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

    protected void reclaim(WriteTransaction txn, SpaceMapEntry txnSpace) {
        synchronized (this) {
            db.pageStore.reclaimAll(txn.getFreed());

            deferredReclaim.addAll(txn.getFreed());
            // TODO: Any reason not to release the transaction at the same time?
            deferredReclaim.add(txnSpace);
        }
    }

    SpaceMapEntry commitTransaction(TransactionPage transactionPage) throws IOException {
        // Shouldn't need to be synchronized (only one concurrent write transaction is allowed), but harmless...
        synchronized (this) {
            PageStore pageStore = db.pageStore;

            transactionPage.setPreviousTransactionPageId(currentTransactionPageId);

            if (deferredReclaim.size() > 64) {
                freeSpaceMap.reclaimAll(deferredReclaim);
                db.pageStore.reclaimAll(deferredReclaim);

                SnapshotWritingPage page = freeSpaceMap.buildSnapshotPageForWrite();

                // ICK: We are likely to write the new snapshot in a newly reclaimed page
                // but... if we need to rollback, we're in trouble...
                SpaceMapEntry fsmPageId = writePageSync(page);
                transactionPage.setFreeSpaceSnapshotId(fsmPageId.getPageId());

                log.info("Wrote FSM snapshot page @{}", fsmPageId.getPageId());

                log.warn("TODO: Handle transaction commit errors with Free Space Map");

                deferredReclaim.clear();
            }

            long transactionId = transactionPage.getTransactionId();

            SpaceMapEntry transactionPageId = writePageSync(transactionPage);

            int newRootPage = transactionPage.getRootPageId();

            pageStore.sync();

            Futures.get(pageStore.writeMasterPage(transactionPage, transactionPageId.getPageId()), IOException.class);

            log.info("Committing transaction {}.  New root={}", transactionId, transactionPage.getRootPageId());

            pageStore.sync();

            setCurrent(newRootPage, transactionId, transactionPageId.getPageId());

            return transactionPageId;
        }
    }

    private SpaceMapEntry writePageSync(Page page) throws IOException {
        return Futures.get(db.pageStore.writePage(this, page), IOException.class);
    }

    public void close() {
        synchronized (this) {
            if (writeTransaction != null) {
                writeTransaction.close();

                writeTransaction = null;
            }

            for (ReadOnlyTransaction txn : readTransactions) {
                txn.close();
            }
            readTransactions.clear();
        }
    }
}
