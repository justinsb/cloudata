package com.cloudata.keyvalue.btree;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

public class ReadWriteTransaction extends Transaction {
    private static final Logger log = LoggerFactory.getLogger(ReadWriteTransaction.class);

    final Map<Integer, TrackedPage> trackedPages = Maps.newHashMap();

    static class TrackedPage {
        final Page page;
        final TrackedPage parent;
        final int originalPageNumber;

        int dirtyCount;

        public TrackedPage(Page page, TrackedPage parent, int originalPageNumber) {
            this.page = page;
            this.parent = parent;
            this.originalPageNumber = originalPageNumber;
        }

    }

    public ReadWriteTransaction(PageStore pageStore) {
        super(pageStore);
    }

    @Override
    public Page getPage(Page parent, int pageNumber) {
        TrackedPage trackedPage = trackedPages.get(pageNumber);
        if (trackedPage == null) {
            TrackedPage trackedParent = null;
            if (parent != null) {
                trackedParent = trackedPages.get(parent.getPageNumber());
            }
            Page page = pageStore.fetchPage(parent, pageNumber);
            trackedPage = new TrackedPage(page, trackedParent, pageNumber);
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

        int newRootPage = -1;

        while (!ready.isEmpty()) {
            TrackedPage trackedPage = ready.remove();

            Page page = trackedPage.page;

            if (trackedPage.parent != null) {
                trackedPage.parent.dirtyCount--;
                assert trackedPage.parent.dirtyCount >= 0;
                if (trackedPage.parent.dirtyCount == 0) {
                    ready.add(trackedPage.parent);
                }
            }

            if (!page.isDirty()) {
                continue;
            }

            int pageNumber = trackedPage.page.getPageNumber();

            int newPageNumber = pageStore.writePage(page);
            // page.changePageNumber(newPageNumber);

            if (trackedPage.parent != null) {
                BranchPage parentPage = (BranchPage) trackedPage.parent.page;

                parentPage.renumberChild(pageNumber, newPageNumber);
            } else {
                // No parent => this must be the root page
                newRootPage = newPageNumber;
            }
        }

        assert newRootPage != -1;

        pageStore.commitTransaction(newRootPage);
    }

    public void insert(ByteBuffer key, ByteBuffer value) {
        getRootPage(true).insert(this, key, value);
    }

    int createdPageCount;

    private void createPage(Page parent, int pageNumber, Page newPage) {
        assert pageNumber == newPage.getPageNumber();
        assert !trackedPages.containsKey(pageNumber);

        TrackedPage trackedParent = null;
        if (parent != null) {
            trackedParent = trackedPages.get(parent.getPageNumber());
        }

        TrackedPage trackedPage = new TrackedPage(newPage, trackedParent, pageNumber);
        trackedPages.put(pageNumber, trackedPage);
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
            createdPageCount++;
            int pageNumber = -createdPageCount;

            LeafPage newPage = LeafPage.createNew(null, pageNumber);
            createPage(null, pageNumber, newPage);

            rootPageId = pageNumber;
        }

        return getPage(null, rootPageId);
    }

}
