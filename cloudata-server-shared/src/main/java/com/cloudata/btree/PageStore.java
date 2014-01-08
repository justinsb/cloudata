package com.cloudata.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.freemap.FreeSpaceMap;
import com.cloudata.freemap.SpaceMapEntry;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public abstract class PageStore {

    private static final Logger log = LoggerFactory.getLogger(PageStore.class);

    private static final boolean DUMP_PAGES = false;

    public static final int PAGENUMBER_MASTER = 0;

    protected static final int HEADER_SIZE = 16384;
    protected static final int MASTERPAGE_SLOTS = 8;

    MasterPage findLatestMasterPage() throws IOException {
        ByteBuffer buffer = Futures.get(readDirect(0, HEADER_SIZE), IOException.class);

        MasterPage latest = null;
        for (int i = 0; i < MASTERPAGE_SLOTS; i++) {
            int position = i * MasterPage.SIZE;
            MasterPage metadataPage = new MasterPage(buffer, position);
            if (latest == null) {
                latest = metadataPage;
            } else if (latest.getTransactionId() < metadataPage.getTransactionId()) {
                latest = metadataPage;
            }
        }

        return latest;
    }

    protected abstract ListenableFuture<ByteBuffer> readDirect(int offset, int length);

    protected abstract ListenableFuture<Void> writeDirect(int offset, ByteBuffer src);

    abstract FreeSpaceMap createEmptyFreeSpaceMap();

    abstract ListenableFuture<PageRecord> fetchPage(Btree btree, Page parent, int pageNumber);

    protected Page buildPage(Btree btree, Page parent, int pageNumber, byte pageType, ByteBuffer pageBuffer) {
        Page page;

        switch (pageType) {
        case BranchPage.PAGE_TYPE:
            page = new BranchPage(btree, parent, pageNumber, pageBuffer);
            break;

        case LeafPage.PAGE_TYPE:
            page = new LeafPage(btree, parent, pageNumber, pageBuffer);
            break;

        case TransactionPage.PAGE_TYPE:
            page = new TransactionPage(btree, parent, pageNumber, pageBuffer);
            break;

        default:
            throw new IllegalStateException();
        }

        if (log.isDebugEnabled()) {
            log.debug("Fetched page {}: {}", pageNumber, page);
        }

        if (DUMP_PAGES) {
            synchronized (System.out) {
                page.dump(System.out);
                System.out.flush();
            }
        }

        return page;
    }

    /**
     * Writes the page to the PageStore (disk, usually)
     * 
     * @param page
     * @return the new page number
     */
    abstract ListenableFuture<SpaceMapEntry> writePage(TransactionTracker tracker, Page page);

    ListenableFuture<Void> writeMasterPage(TransactionPage transactionPage, int transactionPageId) {
        long transactionId = transactionPage.getTransactionId();
        int newRootPage = transactionPage.getRootPageId();

        int slot = (int) (transactionId % MASTERPAGE_SLOTS);

        ByteBuffer mmap = ByteBuffer.allocate(MasterPage.SIZE);
        MasterPage.create(mmap, newRootPage, transactionPageId, transactionId);

        int position = slot * MasterPage.SIZE;
        return writeDirect(position, mmap);
    }

    protected abstract void sync() throws IOException;

    public void reclaimAll(List<SpaceMapEntry> reclaimList) {
    }

    public abstract void debugDump(StringBuilder sb);

    public String debugDump() {
        StringBuilder sb = new StringBuilder();
        debugDump(sb);
        return sb.toString();
    }

    public Optional<Boolean> debugIsIdle() {
        return Optional.absent();
    }
}
