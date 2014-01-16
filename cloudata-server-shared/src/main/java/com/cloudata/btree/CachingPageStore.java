package com.cloudata.btree;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.caching.CacheEntry;
import com.cloudata.btree.caching.PageCache;
import com.cloudata.btree.io.BackingFile;
import com.cloudata.freemap.FreeSpaceMap;
import com.cloudata.freemap.SpaceMapEntry;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class CachingPageStore extends PageStore {
    private static final Logger log = LoggerFactory.getLogger(CachingPageStore.class);

    private static final int ALIGNMENT = 256;

    private static final int GUESS_LENGTH = 4096;

    final BackingFile backingFile;

    final PageCache cache;

    public CachingPageStore(BackingFile backingFile, int cacheSize) throws IOException {
        this.backingFile = backingFile;
        this.cache = new PageCache(cacheSize);

        Preconditions.checkArgument((ALIGNMENT % backingFile.getBlockSize()) == 0);

        readHeader();
    }

    @Override
    public void close() throws IOException {
        cache.close();
        backingFile.close();
    }

    public static CachingPageStore open(BackingFile backingFile) throws IOException {
        // The Guava cache is great, but can evict entries immediately. It isn't really meant for our use case :-)
        // We need a better cache - e.g. one that doesn't throw away referenced buffers
        log.warn("We need a better cache!");
        int cacheSize = 64 * 1024 * 1024;

        return new CachingPageStore(backingFile, cacheSize);
    }

    public static void createNew(BackingFile backingFile) throws IOException {
        {
            ByteBuffer header = buildHeader();
            assert header.remaining() == MASTERPAGE_HEADER_SIZE;
            Futures.get(backingFile.write(header, 0), IOException.class);
        }

        assert MASTERPAGE_SLOT_SIZE > MasterPage.SIZE;

        for (int i = 0; i < MASTERPAGE_SLOTS; i++) {
            int position = MASTERPAGE_HEADER_SIZE + (i * MASTERPAGE_SLOT_SIZE);

            ByteBuffer mmap = ByteBuffer.allocate(MASTERPAGE_SLOT_SIZE);
            assert mmap.position() == 0;
            MasterPage.create(mmap, 0, 0, 0);
            mmap.position(MASTERPAGE_SLOT_SIZE);
            mmap.flip();
            assert mmap.remaining() == MASTERPAGE_SLOT_SIZE;
            Futures.get(backingFile.write(mmap, position), IOException.class);
        }

        backingFile.sync();
    }

    @Override
    public ListenableFuture<PageRecord> fetchPage(Btree btree, Page parent, int pageNumber) {
        return fetchPage(btree, parent, pageNumber, -1);
    }

    ListenableFuture<PageRecord> fetchPage(final Btree btree, final Page parent, final int pageNumber, int length) {
        final int readLength;
        final boolean guessedLength;
        if (length < 0) {
            readLength = GUESS_LENGTH;
            guessedLength = true;
        } else {
            int padding = length % ALIGNMENT;
            if (padding != 0) {
                padding = ALIGNMENT - padding;
            }

            readLength = length + padding;
            guessedLength = false;
        }

        final CacheEntry cacheEntry = cache.allocate(pageNumber, readLength, guessedLength, false);

        final ByteBuf byteBuf = cacheEntry.getBuffer();
        byteBuf.retain();

        assert byteBuf.readerIndex() == 0;
        assert guessedLength || byteBuf.writableBytes() == readLength;

        long offset = pageNumber * (long) ALIGNMENT;

        ListenableFuture<PageRecord> ret = Futures.transform(cacheEntry.read(backingFile, offset),
                new AsyncFunction<ByteBuffer, PageRecord>() {
                    @Override
                    public ListenableFuture<PageRecord> apply(ByteBuffer buffer) throws Exception {
                        assert buffer.position() == 0;

                        PageHeader header = new PageHeader(buffer, buffer.position());

                        int totalSize;

                        SpaceMapEntry space;
                        {
                            int dataSize = header.getDataSize();
                            totalSize = dataSize + PageHeader.HEADER_SIZE;
                            int slots = totalSize / ALIGNMENT;
                            if ((totalSize % ALIGNMENT) != 0) {
                                slots++;
                            }
                            space = new SpaceMapEntry(pageNumber, slots);
                        }

                        if (totalSize <= buffer.remaining()) {
                            Page page = buildPage(btree, parent, pageNumber, header.getPageType(),
                                    header.getPageSlice());

                            // This retains the buffer!
                            PageRecord pageRecord = new PageRecord(page, space, byteBuf);
                            return Futures.immediateFuture(pageRecord);
                        } else {
                            if (!guessedLength) {
                                throw new IllegalStateException();
                            }

                            cache.invalidate(cacheEntry);

                            // TODO: Avoid re-reading the already-read bytes??
                            return fetchPage(btree, parent, pageNumber, totalSize);
                        }
                    }
                });

        Cleanup.add(ret, byteBuf);

        // // The future holds the reference
        // byteBuf.release();

        return ret;
    }

    static final byte[] ZERO = new byte[ALIGNMENT];

    @Override
    ListenableFuture<SpaceMapEntry> writePage(TransactionTracker tracker, Page page) {
        int dataSize = page.getSerializedSize();

        int totalSize = dataSize + PageHeader.HEADER_SIZE;

        int paddedSize;
        int position;
        final SpaceMapEntry allocation;
        {
            int allocateSlots = totalSize / ALIGNMENT;
            if ((totalSize % ALIGNMENT) != 0) {
                allocateSlots++;
            }
            paddedSize = allocateSlots * ALIGNMENT;

            allocation = tracker.allocate(allocateSlots);
            position = allocation.start * ALIGNMENT;
        }

        assert (position % ALIGNMENT) == 0;

        final int pageNumber = allocation.start;
        final CacheEntry cacheEntry = cache.allocate(pageNumber, paddedSize, false, true);

        final ByteBuf backing = cacheEntry.getBuffer();
        backing.retain();

        backing.writerIndex(backing.writerIndex() + paddedSize);
        ByteBuffer nioBuffer = backing.nioBuffer();

        {
            ByteBuffer writeBuffer = nioBuffer.duplicate();
            assert writeBuffer.position() == 0;
            assert writeBuffer.limit() == paddedSize;
            PageHeader.write(writeBuffer, page.getPageType(), dataSize);
            assert writeBuffer.position() == PageHeader.HEADER_SIZE;

            writeBuffer.limit(writeBuffer.position() + dataSize);
            writeBuffer = writeBuffer.slice();

            int pos1 = writeBuffer.position();
            page.write(writeBuffer);
            int pos2 = writeBuffer.position();
            if ((pos2 - pos1) != dataSize) {
                throw new IllegalStateException();
            }

            if (paddedSize != totalSize) {
                writeBuffer = nioBuffer.duplicate();
                writeBuffer.position(PageHeader.HEADER_SIZE + dataSize);
                writeBuffer.put(ZERO, 0, paddedSize - totalSize);
                assert writeBuffer.position() == writeBuffer.limit();
            }
        }

        assert nioBuffer.remaining() == paddedSize;

        // Read-only because we are keeping it in the cache!
        ListenableFuture<Void> writeFuture = backingFile.write(nioBuffer.asReadOnlyBuffer(), position);

        ListenableFuture<SpaceMapEntry> entry = Futures.transform(writeFuture, new Function<Void, SpaceMapEntry>() {
            @Override
            public SpaceMapEntry apply(Void input) {
                cacheEntry.markWriteComplete();

                return allocation;
            }
        });

        Futures.addCallback(entry, new FutureCallback<SpaceMapEntry>() {

            @Override
            public void onSuccess(SpaceMapEntry result) {
            }

            @Override
            public void onFailure(Throwable t) {
                cache.invalidate(cacheEntry);
            }

        });

        Cleanup.add(entry, backing);

        return entry;
    }

    @Override
    protected ListenableFuture<ByteBuffer> readDirect(int offset, int length) {
        assert (offset + length) <= HEADER_SIZE;

        ByteBuffer buffer = ByteBuffer.allocate(length);
        return backingFile.read(buffer, offset);
    }

    @Override
    protected ListenableFuture<Void> writeDirect(int offset, ByteBuffer src) {
        assert (offset + src.remaining()) <= HEADER_SIZE;

        return backingFile.write(src, offset);
    }

    @Override
    public FreeSpaceMap createEmptyFreeSpaceMap() {
        long limit = this.backingFile.size() / ALIGNMENT;
        return FreeSpaceMap.createEmpty(HEADER_SIZE / ALIGNMENT, Ints.checkedCast(limit));
    }

    @Override
    protected void sync() throws IOException {
        // TODO: If we have write buffers, flush them
        this.backingFile.sync();
    }

    @Override
    public void reclaimAll(List<SpaceMapEntry> reclaimList) {
        cache.reclaimAll(reclaimList);
    }

    @Override
    public void debugDump(StringBuilder sb) {
        cache.debugDump(sb);
    }

    @Override
    public Optional<Boolean> debugIsIdle() {
        return Optional.of(cache.debugIsIdle());
    }

    @Override
    public String toString() {
        return "CachingPageStore [backingFile=" + backingFile + "]";
    }

    @Override
    public String getPathInfo() {
        return backingFile.getPathInfo();
    }

}
