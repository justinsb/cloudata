package com.cloudata.btree;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.freemap.SpaceMapEntry;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;

public class WriteTransaction extends Transaction {
    private static final Logger log = LoggerFactory.getLogger(WriteTransaction.class);

    final Map<Integer, TrackedPage> trackedPages = Maps.newHashMap();

    final List<SpaceMapEntry> freed = Lists.newArrayList();
    final List<SpaceMapEntry> allocated = Lists.newArrayList();

    private int rootPageId;

    private long transactionId;

    private boolean committed;

    public boolean isCommitted() {
        return committed;
    }

    public long getTransactionId() {
        return transactionId;
    }

    static class TrackedPage {
        private final PageRecord pageRecord;
        private final Page page;
        TrackedPage parent;
        final int originalPageNumber;

        int dirtyCount;
        final SpaceMapEntry originalFsmEntry;

        private TrackedPage(SpaceMapEntry originalFsmEntry, PageRecord pageRecord, Page page, TrackedPage parent,
                int originalPageNumber) {
            this.originalFsmEntry = originalFsmEntry;
            this.page = page;
            this.pageRecord = pageRecord;
            this.parent = parent;
            this.originalPageNumber = originalPageNumber;

            if (parent != null) {
                parent.dirtyCount++;
            }
        }

        public static TrackedPage forExisting(PageRecord pageRecord, TrackedPage parent) {
            return new TrackedPage(pageRecord.space, pageRecord, pageRecord.page, parent,
                    pageRecord.page.getPageNumber());
        }

        public static TrackedPage forNew(Page page, TrackedPage parent) {
            return new TrackedPage(null, null, page, parent, -1);
        }

        public void close() {
            if (pageRecord != null) {
                pageRecord.release();
            }
        }

        public Page getPage() {
            return this.page;
        }

    }

    public WriteTransaction(Database db, Lock lock, int rootPageId) {
        super(db, lock);
        this.rootPageId = rootPageId;
    }

    @Override
    public Page getPage(Btree btree, Page parent, int pageNumber) {
        TrackedPage trackedPage = trackedPages.get(pageNumber);
        if (trackedPage == null) {
            TrackedPage trackedParent = null;
            if (parent != null) {
                trackedParent = trackedPages.get(parent.getPageNumber());
                if (trackedParent == null) {
                    throw new IllegalStateException();
                }
            }
            PageRecord pageRecord;
            try {
                pageRecord = Futures.get(db.pageStore.fetchPage(btree, parent, pageNumber), IOException.class);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            assert pageRecord.refCnt() == 1;

            trackedPage = TrackedPage.forExisting(pageRecord, trackedParent);
            trackedPages.put(pageNumber, trackedPage);
        }
        return trackedPage.getPage();
    }

    public void commit() throws IOException {
        Queue<TrackedPage> ready = Queues.newArrayDeque();

        for (Entry<Integer, TrackedPage> entry : trackedPages.entrySet()) {
            TrackedPage trackedPage = entry.getValue();
            if (trackedPage.dirtyCount == 0) {
                ready.add(trackedPage);
            }
        }

        if (rootPageId == 0) {
            if (!ready.isEmpty()) {
                throw new IllegalStateException();
            }
            return;
        }

        Integer newRootPage = null;

        while (!ready.isEmpty()) {
            TrackedPage trackedPage = ready.remove();

            Page page = trackedPage.getPage();

            if (page.isDirty()) {
                SpaceMapEntry originalFsmEntry = trackedPage.originalFsmEntry;
                int oldPageNumber = trackedPage.getPage().getPageNumber();

                if (page.shouldSplit()) {
                    BranchPage parentPage;
                    if (trackedPage.parent != null) {
                        parentPage = (BranchPage) trackedPage.parent.getPage();
                    } else {
                        int branchPageNumber = assignPageNumber();

                        parentPage = BranchPage.createNew(page.getBtree(), null, branchPageNumber, page);

                        trackedPage.parent = TrackedPage.forNew(parentPage, null);
                        trackedPage.parent.dirtyCount++;
                    }

                    List<Page> extraPages = parentPage.splitChild(this, oldPageNumber, page);

                    for (Page extraPage : extraPages) {
                        TrackedPage tracked = TrackedPage.forNew(extraPage, trackedPage.parent);
                        ready.add(tracked);
                    }
                }

                // TODO: Any benefit to writing in parallel?
                // TODO: And if not, should we simplify page store to just be synchronous for writes?? (I think reads
                // need to be async because of joining behaviour)
                SpaceMapEntry newEntry = Futures.get(db.pageStore.writePage(db.transactionTracker, page),
                        IOException.class);
                // page.changePageNumber(newPageNumber);
                allocated.add(newEntry);

                int newPageNumber = newEntry.getPageId();
                log.info("Wrote page @{} {}", newPageNumber, page);

                if (trackedPage.parent != null) {
                    BranchPage parentPage = (BranchPage) trackedPage.parent.getPage();

                    parentPage.renumberChild(oldPageNumber, newPageNumber);
                } else {
                    // No parent => this must be the root page
                    assert newRootPage == null;
                    assert page.getParent() == null;

                    newRootPage = newPageNumber;
                }

                if (originalFsmEntry != null) {
                    freed.add(originalFsmEntry);
                }
            }

            if (trackedPage.parent != null) {
                trackedPage.parent.dirtyCount--;
                assert trackedPage.parent.dirtyCount >= 0;
                if (trackedPage.parent.dirtyCount == 0) {
                    ready.add(trackedPage.parent);
                }
            }
        }

        assert newRootPage != null;

        long transactionId = db.transactionTracker.assignTransactionId();
        TransactionPage transactionPage = TransactionPage.createNew(assignPageNumber(), transactionId);
        transactionPage.setRootPageId(newRootPage);
        transactionPage.addToFreed(freed);
        transactionPage.addToAllocated(allocated);

        log.info("Freed pages: {}", Joiner.on(",").join(freed));

        this.spaceMapEntry = db.transactionTracker.commitTransaction(transactionPage);

        this.transactionId = transactionId;
        this.committed = true;
    }

    int createdPageCount;

    private SpaceMapEntry spaceMapEntry;

    public int assignPageNumber() {
        return -(++createdPageCount);
    }

    private void createNewPage(Page parent, int pageNumber, Page newPage) {
        assert pageNumber == newPage.getPageNumber();
        assert !trackedPages.containsKey(pageNumber);

        TrackedPage trackedParent = null;
        if (parent != null) {
            trackedParent = trackedPages.get(parent.getPageNumber());
            if (trackedParent == null) {
                throw new IllegalStateException();
            }
        }

        TrackedPage trackedPage = TrackedPage.forNew(newPage, trackedParent);
        trackedPages.put(pageNumber, trackedPage);
    }

    @Override
    protected Page getRootPage(Btree btree, boolean create) {
        if (rootPageId == 0) {
            if (!create) {
                return null;
            }
            int pageNumber = assignPageNumber();

            LeafPage newPage = LeafPage.createNew(btree, null, pageNumber);
            createNewPage(null, pageNumber, newPage);

            rootPageId = pageNumber;
        }

        return getPage(btree, null, rootPageId);
    }

    public List<SpaceMapEntry> getFreed() {
        return freed;
    }

    public SpaceMapEntry getTransactionSpaceMapEntry() {
        return spaceMapEntry;
    }

    public List<SpaceMapEntry> getAllocated() {
        return allocated;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void close() {
        for (TrackedPage trackedPage : trackedPages.values()) {
            trackedPage.close();
        }

        if (!isCommitted()) {
            db.transactionTracker.freeSpaceMap.reclaimAll(allocated);
            db.pageStore.reclaimAll(allocated);

            allocated.clear();
        }
        super.close();
    }

}
