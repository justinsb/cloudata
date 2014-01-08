package com.cloudata.btree;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.freemap.FreeSpaceMap;
import com.cloudata.freemap.SpaceMapEntry;
import com.cloudata.util.Mmap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class MmapPageStore extends PageStore {
    private static final Logger log = LoggerFactory.getLogger(MmapPageStore.class);

    final MappedByteBuffer buffer;

    private static final int ALIGNMENT = 256;

    private MmapPageStore(MappedByteBuffer buffer) {
        this.buffer = buffer;

        this.buffer.position(HEADER_SIZE);
    }

    public static MmapPageStore build(File data) throws IOException {
        if (!data.exists()) {
            long size = 1024L * 1024L * 64L;
            MappedByteBuffer mmap = Mmap.mmapFile(data, size);

            for (int i = 0; i < MASTERPAGE_SLOTS; i++) {
                mmap.position(i * MasterPage.SIZE);
                MasterPage.create(mmap, 0, 0, 0);
            }

            return new MmapPageStore(mmap);
        } else {
            long size = data.length();
            MappedByteBuffer mmap = Mmap.mmapFile(data, size);

            return new MmapPageStore(mmap);
        }
    }

    @Override
    public ListenableFuture<PageRecord> fetchPage(Btree btree, Page parent, int pageNumber) {
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

        Page page = buildPage(btree, parent, pageNumber, header.getPageType(), header.getPageSlice());

        return Futures.immediateFuture(new PageRecord(page, space, null));
    }

    @Override
    ListenableFuture<SpaceMapEntry> writePage(TransactionTracker tracker, Page page) {
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
            allocation = tracker.allocate(allocateSlots);
            position = allocation.start * ALIGNMENT;
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

        return Futures.immediateFuture(allocation);
    }

    @Override
    protected ListenableFuture<ByteBuffer> readDirect(int offset, int length) {
        assert (offset + length) <= HEADER_SIZE;

        ByteBuffer mmap = this.buffer.duplicate();
        mmap.position(offset);
        mmap.limit(offset + length);
        return Futures.immediateFuture(mmap.slice());
    }

    @Override
    protected ListenableFuture<Void> writeDirect(int offset, ByteBuffer src) {
        assert (offset + src.remaining()) <= HEADER_SIZE;

        ByteBuffer mmap = this.buffer.duplicate();
        mmap.position(offset);
        mmap.put(src);

        return Futures.immediateFuture(null);
    }

    @Override
    public FreeSpaceMap createEmptyFreeSpaceMap() {
        return FreeSpaceMap.createEmpty(HEADER_SIZE / ALIGNMENT, this.buffer.limit() / ALIGNMENT);
    }

    @Override
    protected void sync() {
        this.buffer.force();
    }

    @Override
    public void debugDump(StringBuilder sb) {
        sb.append(this.toString());
    }

    @Override
    public String toString() {
        return "MmapPageStore [buffer=" + buffer + "]";
    }

}
