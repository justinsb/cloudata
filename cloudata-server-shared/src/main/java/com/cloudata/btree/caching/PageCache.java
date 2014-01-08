package com.cloudata.btree.caching;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.freemap.SpaceMapEntry;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.collect.Lists;

public class PageCache {

    private static final Logger log = LoggerFactory.getLogger(PageCache.class);

    final Cache<Integer, CacheEntry> cache;

    final PooledByteBufAllocator pool;

    public PageCache(long cacheSize) {
        // TODO: Replace with tighter pool (split up a big chunk of RAM)?
        this.pool = new PooledByteBufAllocator();

        // TODO: Replace with better cache; e.g. one that actually considers whether entries are in use!
        this.cache = CacheBuilder.newBuilder().weigher(new Weigher<Integer, CacheEntry>() {
            @Override
            public int weigh(Integer key, CacheEntry value) {
                return value.weight();
            }
        }).maximumWeight(cacheSize).removalListener(new RemovalListener<Integer, CacheEntry>() {
            @Override
            public void onRemoval(RemovalNotification<Integer, CacheEntry> notification) {
                log.info("CachingPageStore removal: {}", notification.getKey());

                CacheEntry value = notification.getValue();
                value.close();
            }
        }).build();
    }

    public CacheEntry allocate(final int pageId, final int length, boolean lengthIsGuess, final boolean forWrite) {
        try {
            Preconditions.checkArgument(pageId > 0);

            log.info("CachingPageStore get page {} length {}", pageId, length);

            CacheEntry entry = cache.get(pageId, new Callable<CacheEntry>() {
                @Override
                public CacheEntry call() throws Exception {
                    ByteBuf buffer = pool.ioBuffer(length, length);
                    assert buffer.isDirect();
                    assert buffer.capacity() == length;
                    assert buffer.readerIndex() == 0;
                    assert buffer.writerIndex() == 0;
                    assert buffer.refCnt() == 1;

                    CacheEntry entry = new CacheEntry(forWrite ? CacheEntry.State.READY_FOR_WRITE
                            : CacheEntry.State.READY_FOR_READ, pageId, length, buffer);
                    return entry;
                }
            });

            // Sanity check
            Preconditions.checkState(lengthIsGuess || entry.size() == length);
            switch (entry.getState()) {
            case HAVE_DATA: // We shouldn't write into existing slot
            case READY_FOR_READ:
                Preconditions.checkState(!forWrite);
                break;

            case READY_FOR_WRITE:
                Preconditions.checkState(forWrite);
                break;

            default:
                throw new IllegalStateException();
            }

            return entry;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public void invalidate(CacheEntry cacheEntry) {
        cache.invalidate(cacheEntry.pageNumber);
    }

    public void reclaimAll(List<SpaceMapEntry> reclaimList) {
        List<Integer> keys = Lists.newArrayList();

        for (SpaceMapEntry entry : reclaimList) {
            keys.add(entry.start);
        }

        log.info("Reclaim pages: {}", keys);
        cache.invalidateAll(keys);
    }

    public void debugDump(StringBuilder sb) {
        for (Entry<Integer, CacheEntry> entry : cache.asMap().entrySet()) {
            sb.append(entry.getKey());
            sb.append("\t");
            sb.append(entry.getValue());
            sb.append("\n");
        }
    }

    public boolean debugIsIdle() {
        for (Entry<Integer, CacheEntry> entry : cache.asMap().entrySet()) {
            CacheEntry cacheEntry = entry.getValue();
            if (cacheEntry.getBuffer().refCnt() != 1) {
                assert cacheEntry.getBuffer().refCnt() > 0;
                return false;
            }
        }
        return true;
    }
}
