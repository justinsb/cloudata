package com.cloudata.keyvalue.btree;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.freemap.FreeSpaceMap;
import com.cloudata.keyvalue.freemap.SpaceMapEntry;
import com.cloudata.util.Mmap;
import com.google.common.collect.Lists;

public class MmapPageStore extends PageStore {

    private static final Logger log = LoggerFactory.getLogger(MmapPageStore.class);

    final MappedByteBuffer buffer;

    final boolean uniqueKeys;

    final FreeSpaceMap freeSpaceMap;

    final List<SpaceMapEntry> deferredReclaim;

    private static final int ALIGNMENT = 256;

    private static final int HEADER_SIZE = 16384;

    private static final int MASTERPAGE_SLOTS = 8;

    private MmapPageStore(MappedByteBuffer buffer, boolean uniqueKeys) {
        this.buffer = buffer;
        this.uniqueKeys = uniqueKeys;
        this.deferredReclaim = Lists.newArrayList();

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

        this.nextTransactionId = latest.getTransactionId() + 1;
        setCurrent(latest.getRoot(), latest.getTransactionId(), latest.getTransactionPageId());

        this.freeSpaceMap = recoverFreeSpaceMap(latest);

        this.buffer.position(HEADER_SIZE);
    }

    private FreeSpaceMap recoverFreeSpaceMap(MasterPage latest) {
        int transactionPageId = latest.getTransactionPageId();
        List<PageRecord> history = Lists.newArrayList();
        PageRecord fsmSnapshot = null;

        // Walk the list of transactions backwards until we find a FSM snapshot
        if (transactionPageId != 0) {
            PageRecord current = fetchPage(null, transactionPageId);

            while (true) {
                TransactionPage transactionPage = (TransactionPage) current.page;
                history.add(current);
                if (transactionPage.getFreeSpaceSnapshotId() != 0) {
                    fsmSnapshot = fetchPage(null, transactionPage.getFreeSpaceSnapshotId());
                    break;
                }

                int previousTransactionPageId = transactionPage.getPreviousTransactionPageId();
                if (previousTransactionPageId == 0) {
                    break;
                }
                PageRecord previous = fetchPage(null, previousTransactionPageId);
                assert (previous != null);
                current = previous;
            }

            Collections.reverse(history);
        }

        FreeSpaceMap fsm;
        if (fsmSnapshot == null) {
            fsm = FreeSpaceMap.createEmpty(HEADER_SIZE / ALIGNMENT, this.buffer.limit() / ALIGNMENT);
        } else {
            fsm = FreeSpaceMap.createFromSnapshot(fsmSnapshot);
        }

        for (PageRecord txnRecord : history) {
            fsm.replay(txnRecord);
        }

        return fsm;
    }

    public static MmapPageStore build(File data, boolean uniqueKeys) throws IOException {
        if (!data.exists()) {
            long size = 1024L * 1024L * 64L;
            MappedByteBuffer mmap = Mmap.mmapFile(data, size);

            for (int i = 0; i < MASTERPAGE_SLOTS; i++) {
                mmap.position(i * MasterPage.SIZE);
                MasterPage.create(mmap, 0, 0, 0);
            }

            return new MmapPageStore(mmap, uniqueKeys);
        } else {
            long size = data.length();
            MappedByteBuffer mmap = Mmap.mmapFile(data, size);

            return new MmapPageStore(mmap, uniqueKeys);
        }
    }

    @Override
    public PageRecord fetchPage(Page parent, int pageNumber) {
        int offset = pageNumber * ALIGNMENT;

        PageHeader header = new PageHeader(buffer, offset);

        SpaceMapEntry space;

        {
            int dataSize = header.getDataSize();
            int totalSize = dataSize + PageHeader.HEADER_SIZE;
            int slots = totalSize / ALIGNMENT;
            if ((totalSize % ALIGNMENT) != 0) {
                slots++;
            }
            space = new SpaceMapEntry(pageNumber, slots);
        }

        Page page;

        switch (header.getPageType()) {
        case BranchPage.PAGE_TYPE:
            page = new BranchPage(parent, pageNumber, header.getPageSlice());
            break;

        case LeafPage.PAGE_TYPE:
            page = new LeafPage(parent, pageNumber, header.getPageSlice(), uniqueKeys);
            break;

        case TransactionPage.PAGE_TYPE:
            page = new TransactionPage(parent, pageNumber, header.getPageSlice());
            break;

        default:
            throw new IllegalStateException();
        }

        log.info("Fetched page {}: {}", pageNumber, page);

        synchronized (System.out) {
            page.dump(System.out);
            System.out.flush();
        }

        return new PageRecord(page, space);
    }

    @Override
    public SpaceMapEntry writePage(Page page) {
        int dataSize = page.getSerializedSize();

        int totalSize = dataSize + PageHeader.HEADER_SIZE;

        // int padding = totalSize % ALIGNMENT;
        // if (padding != 0) {
        // padding = ALIGNMENT - padding;
        // }

        int position;
        SpaceMapEntry allocation;
        {
            int allocateSlots = totalSize / ALIGNMENT;
            if ((totalSize % ALIGNMENT) != 0) {
                allocateSlots++;
            }
            int allocated = freeSpaceMap.allocate(allocateSlots);
            if (allocated < 0) {
                // TODO: Grow database
                throw new IllegalStateException();
            }

            position = allocated * ALIGNMENT;
            allocation = new SpaceMapEntry(allocated, allocateSlots);
        }

        // if (totalSize > buffer.remaining()) {
        // // TODO: Reclaim old space
        // // TODO: Incorporate padding into calculation?
        // throw new UnsupportedOperationException();
        // }
        //
        // int position = buffer.position();
        assert (position % ALIGNMENT) == 0;

        // int newPageNumber = (position + (ALIGNMENT - 1)) / ALIGNMENT;
        // int newPageNumber = position / ALIGNMENT;

        ByteBuffer writeBuffer = buffer.duplicate();
        writeBuffer.position(position);
        PageHeader.write(writeBuffer, page.getPageType(), dataSize);

        writeBuffer.limit(writeBuffer.position() + dataSize);
        writeBuffer = writeBuffer.slice();

        // writeBuffer.position(buffer.position())

        int pos1 = writeBuffer.position();
        page.write(writeBuffer);
        int pos2 = writeBuffer.position();
        if ((pos2 - pos1) != dataSize) {
            throw new IllegalStateException();
        }

        // buffer.position(buffer.position() + totalSize + padding);
        // assert (buffer.position() % ALIGNMENT) == 0;

        return allocation;
    }

    @Override
    public SpaceMapEntry commitTransaction(TransactionPage transactionPage) {
        // Shouldn't need to be synchronized, but harmless...
        synchronized (this) {
            transactionPage.setPreviousTransactionPageId(currentTransactionPage);

            if (deferredReclaim.size() > 64) {
                freeSpaceMap.reclaimAll(deferredReclaim);

                SpaceMapEntry fsmPageId = freeSpaceMap.writeSnapshot(this);
                transactionPage.setFreeSpaceSnapshotId(fsmPageId.getPageId());

                deferredReclaim.clear();
            }

            long transactionId = transactionPage.getTransactionId();

            SpaceMapEntry transactionPageId = writePage(transactionPage);

            int newRootPage = transactionPage.getRootPageId();

            int slot = (int) (transactionId % MASTERPAGE_SLOTS);

            int position = slot * MasterPage.SIZE;

            ByteBuffer mmap = this.buffer.duplicate();
            mmap.position(position);

            MasterPage.create(mmap, newRootPage, transactionPageId.getPageId(), transactionId);

            log.info("Committing transaction {}.  New root={}", transactionId, newRootPage);

            setCurrent(newRootPage, transactionId, transactionPageId.getPageId());

            return transactionPageId;
        }
    }

    @Override
    protected void reclaim(WriteTransaction txn, SpaceMapEntry txnSpace) {
        synchronized (this) {
            deferredReclaim.addAll(txn.getFreed());
            // TODO: Any reason not to release the transaction at the same time?
            deferredReclaim.add(txnSpace);
        }
    }

}
