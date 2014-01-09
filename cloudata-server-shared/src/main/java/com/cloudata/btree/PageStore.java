package com.cloudata.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.freemap.FreeSpaceMap;
import com.cloudata.freemap.FreeSpaceMap.SnapshotPage;
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
    protected static final int MASTERPAGE_HEADER_SIZE = 512;
    protected static final int MASTERPAGE_SLOT_SIZE = 256;

    private static final int MASTERPAGE_HEADER_MAGIC = 0xC10DDA7A;

    protected void readHeader() throws IOException {
        // Because of crypto IV, we cannot read in bulk...
        int position = 0;
        ByteBuffer buffer = Futures.get(readDirect(position, MASTERPAGE_HEADER_SIZE), IOException.class);
        assert buffer.remaining() == MASTERPAGE_HEADER_SIZE;

        int magic = buffer.getInt();
        int formatVersion = buffer.getInt();

        if (magic != MASTERPAGE_HEADER_MAGIC) {
            throw new IOException("Corrupted header");
        }

        if (formatVersion != 1) {
            throw new IOException("Unknown version");
        }
    }

    protected static ByteBuffer buildHeader() {
        ByteBuffer b = ByteBuffer.allocate(MASTERPAGE_HEADER_SIZE);
        b.putInt(MASTERPAGE_HEADER_MAGIC);
        b.putInt(1);

        b.position(MASTERPAGE_HEADER_SIZE);

        b.flip();
        return b;
    }

    MasterPage findLatestMasterPage() throws IOException {
        MasterPage latest = null;
        for (int i = 0; i < MASTERPAGE_SLOTS; i++) {
            // Because of crypto IV, we cannot read in bulk...
            int position = MASTERPAGE_HEADER_SIZE + (i * MASTERPAGE_SLOT_SIZE);
            ByteBuffer buffer = Futures.get(readDirect(position, MASTERPAGE_SLOT_SIZE), IOException.class);

            MasterPage masterPage = new MasterPage(buffer, 0);
            if (!masterPage.isValid()) {
                log.warn("Found corrupted master page slot {}", i);
                continue;
            }

            if (latest == null) {
                latest = masterPage;
            } else if (latest.getTransactionId() < masterPage.getTransactionId()) {
                latest = masterPage;
            }
        }

        if (latest == null) {
            throw new IOException("All master pages corrupted");
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
            assert btree == null;
            assert parent == null;
            page = new TransactionPage(btree, parent, pageNumber, pageBuffer);
            break;

        case SnapshotPage.PAGE_TYPE:
            assert btree == null;
            assert parent == null;
            page = new SnapshotPage(pageNumber, pageBuffer);
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

        {
            int position = MASTERPAGE_HEADER_SIZE + (slot * MASTERPAGE_SLOT_SIZE);

            ByteBuffer mmap = ByteBuffer.allocate(MASTERPAGE_SLOT_SIZE);
            assert mmap.position() == 0;
            MasterPage.create(mmap, newRootPage, transactionPageId, transactionId);
            mmap.position(MASTERPAGE_SLOT_SIZE);
            mmap.flip();
            assert mmap.remaining() == MASTERPAGE_SLOT_SIZE;
            return writeDirect(position, mmap);
        }
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

    public void close() throws IOException {
        // Note: we don't do a sync - that is driven by transactions

    }

}
