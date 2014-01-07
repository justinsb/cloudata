package com.cloudata.btree;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.freemap.FreeSpaceMap;
import com.cloudata.freemap.SpaceMapEntry;

public abstract class PageStore {

    private static final Logger log = LoggerFactory.getLogger(PageStore.class);

    private static final boolean DUMP_PAGES = false;

    public static final int PAGENUMBER_MASTER = 0;

    protected static final int HEADER_SIZE = 16384;
    protected static final int MASTERPAGE_SLOTS = 8;

    MasterPage findLatestMasterPage() {
        ByteBuffer buffer = readDirect(0, HEADER_SIZE);

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

    protected abstract ByteBuffer readDirect(int offset, int length);

    protected abstract void writeDirect(int offset, ByteBuffer src);

    abstract FreeSpaceMap createEmptyFreeSpaceMap();

    abstract PageRecord fetchPage(Btree btree, Page parent, int pageNumber);

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
    abstract SpaceMapEntry writePage(TransactionTracker tracker, Page page);

    void writeMasterPage(TransactionPage transactionPage, int transactionPageId) {
        long transactionId = transactionPage.getTransactionId();
        int newRootPage = transactionPage.getRootPageId();

        int slot = (int) (transactionId % MASTERPAGE_SLOTS);

        ByteBuffer mmap = ByteBuffer.allocate(MasterPage.SIZE);
        MasterPage.create(mmap, newRootPage, transactionPageId, transactionId);

        int position = slot * MasterPage.SIZE;
        writeDirect(position, mmap);
    }

    protected abstract void sync();
}
