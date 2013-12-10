package com.cloudata.keyvalue.btree;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.util.Mmap;

public class MmapPageStore extends PageStore {

    private static final Logger log = LoggerFactory.getLogger(MmapPageStore.class);

    final MappedByteBuffer buffer;

    final boolean uniqueKeys;

    private static final int ALIGNMENT = 256;

    private static final int HEADER_SIZE = 16384;

    private static final int MASTERPAGE_SLOTS = 8;

    private MmapPageStore(MappedByteBuffer buffer, boolean uniqueKeys) {
        this.buffer = buffer;
        this.uniqueKeys = uniqueKeys;

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
        setCurrent(latest.getRoot(), latest.getTransactionPageId());

        this.buffer.position(HEADER_SIZE);
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
    public Page fetchPage(Page parent, int pageNumber) {
        int offset = pageNumber * ALIGNMENT;

        PageHeader header = new PageHeader(buffer, offset);

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

        return page;
    }

    @Override
    public int writePage(Page page) {
        int dataSize = page.getSerializedSize();

        int totalSize = dataSize + PageHeader.HEADER_SIZE;

        if (totalSize > buffer.remaining()) {
            // TODO: Reclaim old space
            // TODO: Incorporate padding into calculation?
            throw new UnsupportedOperationException();
        }

        int position = buffer.position();
        assert (position % ALIGNMENT) == 0;

        // int newPageNumber = (position + (ALIGNMENT - 1)) / ALIGNMENT;
        int newPageNumber = position / ALIGNMENT;

        ByteBuffer writeBuffer = buffer.duplicate();
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

        int padding = totalSize % ALIGNMENT;
        if (padding != 0) {
            padding = ALIGNMENT - padding;
        }
        buffer.position(buffer.position() + totalSize + padding);
        assert (buffer.position() % ALIGNMENT) == 0;

        return newPageNumber;
    }

    @Override
    public void commitTransaction(TransactionPage transactionPage) {
        // Shouldn't need to be synchronized, but harmless...
        synchronized (this) {
            transactionPage.setPreviousTransactionPage(currentTransactionPage);

            long transactionId = transactionPage.getTransactionId();

            int transactionPageId = writePage(transactionPage);

            int newRootPage = transactionPage.getRootPage();

            int slot = transactionPageId % MASTERPAGE_SLOTS;

            int position = slot * MasterPage.SIZE;

            ByteBuffer mmap = this.buffer.duplicate();
            mmap.position(position);

            MasterPage.create(mmap, newRootPage, transactionPageId, transactionId);

            log.info("Committing transaction {}.  New root={}", transactionId, newRootPage);

            setCurrent(newRootPage, transactionPageId);
        }
    }

}
