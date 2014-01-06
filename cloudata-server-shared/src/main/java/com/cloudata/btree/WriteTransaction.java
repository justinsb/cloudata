package com.cloudata.btree;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.PageStore.PageRecord;
import com.cloudata.freemap.SpaceMapEntry;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

public class WriteTransaction extends Transaction {
    private static final Logger log = LoggerFactory.getLogger(WriteTransaction.class);

    final Map<Integer, TrackedPage> trackedPages = Maps.newHashMap();

    final List<SpaceMapEntry> freed = Lists.newArrayList();
    final List<SpaceMapEntry> allocated = Lists.newArrayList();

    private int rootPageId;

    private long transactionId;

    public long getTransactionId() {
        return transactionId;
    }

    static class TrackedPage {
        final Page page;
        TrackedPage parent;
        final int originalPageNumber;

        int dirtyCount;
        final SpaceMapEntry originalFsmEntry;

        public TrackedPage(SpaceMapEntry originalFsmEntry, Page page, TrackedPage parent, int originalPageNumber) {
            this.originalFsmEntry = originalFsmEntry;
            this.page = page;
            this.parent = parent;
            this.originalPageNumber = originalPageNumber;

            if (parent != null) {
                parent.dirtyCount++;
            }
        }
    }

    public WriteTransaction(PageStore pageStore, Lock lock, int rootPageId) {
        super(pageStore, lock);
        this.rootPageId = rootPageId;
    }

    @Override
    public Page getPage(Page parent, int pageNumber) {
        TrackedPage trackedPage = trackedPages.get(pageNumber);
        if (trackedPage == null) {
            TrackedPage trackedParent = null;
            if (parent != null) {
                trackedParent = trackedPages.get(parent.getPageNumber());
                if (trackedParent == null) {
                    throw new IllegalStateException();
                }
            }
            PageRecord pageRecord = pageStore.fetchPage(parent, pageNumber);
            trackedPage = new TrackedPage(pageRecord.space, pageRecord.page, trackedParent, pageNumber);
            trackedPages.put(pageNumber, trackedPage);
        }
        return trackedPage.page;
    }

    public void commit() {
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

            Page page = trackedPage.page;

            if (page.isDirty()) {
                SpaceMapEntry originalFsmEntry = trackedPage.originalFsmEntry;
                int oldPageNumber = trackedPage.page.getPageNumber();

                if (page.shouldSplit()) {
                    BranchPage parentPage;
                    if (trackedPage.parent != null) {
                        parentPage = (BranchPage) trackedPage.parent.page;
                    } else {
                        int branchPageNumber = assignPageNumber();

                        parentPage = BranchPage.createNew(null, branchPageNumber, page);

                        trackedPage.parent = new TrackedPage(null, parentPage, null, branchPageNumber);
                        trackedPage.parent.dirtyCount++;
                    }

                    List<Page> extraPages = parentPage.splitChild(this, oldPageNumber, page);

                    for (Page extraPage : extraPages) {
                        TrackedPage tracked = new TrackedPage(null, extraPage, trackedPage.parent,
                                extraPage.getPageNumber());
                        ready.add(tracked);
                    }
                }

                SpaceMapEntry newEntry = pageStore.writePage(page);
                // page.changePageNumber(newPageNumber);
                allocated.add(newEntry);

                int newPageNumber = newEntry.getPageId();
                log.info("Wrote page @{} {}", newPageNumber, page);

                if (trackedPage.parent != null) {
                    BranchPage parentPage = (BranchPage) trackedPage.parent.page;

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

        long transactionId = pageStore.assignTransactionId();
        TransactionPage transactionPage = TransactionPage.createNew(assignPageNumber(), transactionId);
        transactionPage.setRootPageId(newRootPage);
        transactionPage.addToFreed(freed);
        transactionPage.addToAllocated(allocated);

        log.info("Freed pages: {}", Joiner.on(",").join(freed));

        this.spaceMapEntry = pageStore.commitTransaction(transactionPage);

        this.transactionId = transactionId;
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

        SpaceMapEntry spaceMapEntry = null;
        TrackedPage trackedPage = new TrackedPage(spaceMapEntry, newPage, trackedParent, pageNumber);
        trackedPages.put(pageNumber, trackedPage);
    }

    @Override
    protected Page getRootPage(Btree btree, boolean create) {
        if (rootPageId == 0) {
            if (!create) {
                return null;
            }
            int pageNumber = assignPageNumber();

            LeafPage newPage = LeafPage.createNew(null, pageNumber, btree.isUniqueKeys());
            createNewPage(null, pageNumber, newPage);

            rootPageId = pageNumber;
        }

        return getPage(null, rootPageId);
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

}
