package com.cloudata.blockstore.backend.cloud;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.blockstore.Volume;
import com.cloudata.blockstore.VolumeProto.VolumeData;
import com.cloudata.clients.keyvalue.KeyValueStore;
import com.cloudata.files.blobs.BlobStore;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

public class CloudVolume implements Volume {

    // TODO: SUPPORT iscsi_unmap_task

    private static final Logger log = LoggerFactory.getLogger(CloudVolume.class);

    private static final int CHUNK_SIZE = 256 * 1024;

    private final long size;

    final int segmentSize;
    final int segmentCount;

    final LoadingCache<Integer, Segment> segmentCache;
    final KeyValueStore keyValueStore;
    final ThreadPools executors;
    final BlobStore blobStore;

    final Object lock = new Object();

    final Set<Segment> dirtySegments = Sets.newHashSet();

    public CloudVolume(ThreadPools executors, KeyValueStore keyValueStore, BlobStore blobStore, VolumeData volumeData) {
        this.executors = executors;
        this.keyValueStore = keyValueStore;
        this.blobStore = blobStore;

        SegmentCacheLoader loader = new SegmentCacheLoader();
        this.segmentCache = CacheBuilder.newBuilder().recordStats().build(loader);

        this.segmentSize = volumeData.getSegmentSize();
        this.segmentCount = volumeData.getSegmentCount();

        this.size = (long) segmentCount * (long) segmentSize;
    }

    @Override
    public ListenableFuture<ByteBuf> read(long offset, final long length) {
        Preconditions.checkArgument(length >= 0);

        log.warn("MMAP: read {} {}", offset, length);

        int segmentIndex = Ints.checkedCast(offset / segmentSize);
        long segmentStart = (long) segmentIndex * (long) segmentSize;

        List<ListenableFuture<ByteBuf>> futures = Lists.newArrayList();

        long remaining = length;
        while (remaining > 0) {
            long segmentEnd = segmentStart + segmentSize;

            assert segmentStart <= offset;
            assert segmentEnd > offset;

            int n = (int) Math.min(remaining, segmentEnd - offset);
            assert n > 0;

            Segment segment = getSegment(segmentIndex);

            int segmentOffset = (int) (offset - segmentStart);
            ListenableFuture<ByteBuf> data = segment.read(segmentOffset, n);

            futures.add(data);

            offset += n;
            remaining -= n;
            segmentIndex++;
            segmentStart += segmentSize;
        }

        assert remaining == 0;

        return Futures.transform(Futures.allAsList(futures), new Function<List<ByteBuf>, ByteBuf>() {

            @Override
            public ByteBuf apply(List<ByteBuf> input) {
                ByteBuf retval = ByteBufs.combine(input);
                assert retval.readableBytes() == length;
                return retval;
            }

        });
    }

    private Segment getSegment(int segment) {
        try {
            return segmentCache.get(segment);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void write(long offset, long length, ByteBuf buf) {
        Preconditions.checkState(length == buf.readableBytes());

        log.warn("VOLUME: write {} {}", offset, length);

        int segmentIndex = Ints.checkedCast(offset / segmentSize);
        long segmentStart = (long) segmentIndex * (long) segmentSize;

        int bufferOffset = buf.readerIndex();

        while (length != 0) {
            long segmentEnd = segmentStart + segmentSize;

            assert segmentStart <= offset;
            assert segmentEnd > offset;

            int n = (int) Math.min(length, segmentEnd - offset);
            assert n != 0;

            Segment segment = getSegment(segmentIndex);

            int segmentOffset = (int) (offset - segmentStart);
            ByteBuf slice = buf.slice(bufferOffset, n);

            segment.write(segmentOffset, slice);

            synchronized (lock) {
                dirtySegments.add(segment);
            }

            offset += n;
            length -= n;
            bufferOffset += n;
            segmentIndex++;
            segmentStart += segmentSize;
        }

    }

    @Override
    public ListenableFuture<Void> sync() {
        log.warn("CLOUD: sync");

        List<Segment> dirtySegmentsSnapshot;

        synchronized (lock) {
            dirtySegmentsSnapshot = Lists.newArrayList(dirtySegments);

            List<ListenableFuture<Void>> futures = Lists.newArrayList();
            for (Segment segment : dirtySegmentsSnapshot) {
                ListenableFuture<Void> future = segment.sync();
                futures.add(future);
            }

            return Futures.transform(Futures.allAsList(futures), new Function<List<Void>, Void>() {
                @Override
                public Void apply(List<Void> input) {
                    synchronized (lock) {
                        Iterator<Segment> it = dirtySegments.iterator();
                        while (it.hasNext()) {
                            Segment segment = it.next();
                            if (!segment.isDirty()) {
                                it.remove();
                            }
                        }
                    }
                    return null;
                }
            });
        }
    }

    @Override
    public int getChunkSize() {
        return CHUNK_SIZE;
    }

    @Override
    public long getLength() {
        return size;
    }

    @Immutable
    final class SegmentCacheLoader extends CacheLoader<Integer, Segment> {

        @Override
        public Segment load(@Nonnull Integer segment) throws Exception {
            assert segment != null;

            try {
                ByteString segmentKey = getKeyValueKey(segment);

                return new Segment(CloudVolume.this, segment, segmentKey);
            } catch (Exception e) {
                log.warn("Error building KeyValueStore", e);
                throw e;
            }
        }
    }

    public ByteString getKeyValueKey(int segment) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(segment);
        buffer.flip();
        return ByteString.copyFrom(buffer);
    }

    ByteString getBlobPrefix() {
        return ByteString.EMPTY;
    }

}
