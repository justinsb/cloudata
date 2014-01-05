package com.cloudata.blockstore.backend.cloud;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.blockstore.VolumeProto.ChunkData;
import com.cloudata.blockstore.VolumeProto.SegmentData;
import com.cloudata.blockstore.bytebuf.ByteBufByteSource;
import com.cloudata.blockstore.bytebuf.ExternalByteBuf;
import com.cloudata.clients.keyvalue.IfNotExists;
import com.cloudata.clients.keyvalue.IfVersion;
import com.cloudata.clients.keyvalue.Modifier;
import com.cloudata.files.blobs.BlobCache;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

public class Segment {
    private static final Logger log = LoggerFactory.getLogger(Segment.class);

    final CloudVolume cloudVolume;
    final long volumeOffset;
    final ByteString segmentKey;

    final Object lock = new Object();

    final TreeMap<Integer, Chunk> chunks = Maps.newTreeMap();
    private Object currentKeyValueVersion;

    long version;
    long wroteVersion;

    final Map<Long, ListenableFuture<Void>> syncOperations = Maps.newHashMap();
    final Map<Chunk, ListenableFuture<ByteString>> writeOperations = Maps.newHashMap();

    public Segment(CloudVolume cloudVolume, int segment, ByteString segmentKey) throws IOException {
        assert segmentKey.size() > 0;

        this.cloudVolume = cloudVolume;
        this.volumeOffset = segment * cloudVolume.segmentSize;
        this.segmentKey = segmentKey;

        ByteString segmentData = cloudVolume.keyValueStore.read(SPACE_SEGMENTS, segmentKey);
        this.currentKeyValueVersion = segmentData;

        if (segmentData != null) {
            parse(SegmentData.parseFrom(segmentData));
        }
    }

    private void parse(SegmentData segmentData) {
        if (segmentData.getOffset() != volumeOffset) {
            throw new IllegalStateException();
        }

        for (ChunkData chunkData : segmentData.getChunkList()) {
            BlobChunk chunk = new BlobChunk(chunkData.getStart(), chunkData.getLength(), chunkData.getHash(),
                    chunkData.getSkip());
            chunks.put(chunk.start, chunk);
        }
    }

    static abstract class Chunk implements Closeable {
        final int start;
        final int length;

        private boolean closed;

        public Chunk(int start, int length) {
            this.start = start;
            this.length = length;
        }

        public int end() {
            return start + length;
        }

        @Override
        public void close() {
            assert !closed;
            closed = true;
        }

        boolean isClosed() {
            return closed;
        }

        public boolean entirelyContains(Chunk child) {
            if (child.start < this.start) {
                return false;
            }
            if (child.end() > this.end()) {
                return false;
            }
            return true;
        }

        public boolean overlaps(Chunk other) {
            if (this.end() <= other.start) {
                return false;
            }
            if (this.start >= other.end()) {
                return false;
            }
            return true;
        }

        public boolean isEmpty() {
            return length == 0;
        }

        public Chunk slice(int newStart) {
            assert this.start != newStart;
            return slice(newStart, end() - newStart);
        }

        public abstract Chunk slice(int newStart, int newLength);
    }

    static class BlobChunk extends Chunk {
        final ByteString hash;
        final int skip;

        public BlobChunk(int start, int length, ByteString hash, int skip) {
            super(start, length);

            this.hash = hash;
            this.skip = skip;

            assert hash != null;
        }

        @Override
        public Chunk slice(int newStart, int newLength) {
            Preconditions.checkArgument(length > 0);

            int newSkip = newStart - start;
            Preconditions.checkArgument((newSkip >= 0) && (newSkip <= length));
            Preconditions.checkArgument((newSkip > 0) || (newLength < length));
            newSkip += this.skip;

            return new BlobChunk(newStart, newLength, hash, newSkip);
        }

        @Override
        public String toString() {
            return "BlobChunk [skip=" + skip + ", start=" + start + ", length=" + length + ", hash="
                    + BaseEncoding.base16().encode(hash.toByteArray()) + "]";
        }

    }

    static class ByteBufChunk extends Chunk {
        final ByteBuf data;

        public ByteBufChunk(int start, int length, ByteBuf data) {
            super(start, length);

            this.data = data.duplicate();
            this.data.retain();

            assert length == this.data.readableBytes();
        }

        @Override
        public void close() {
            super.close();

            this.data.release();
        }

        @Override
        public Chunk slice(int newStart, int newLength) {
            Preconditions.checkArgument(length > 0);

            int skip = newStart - start;
            Preconditions.checkArgument((skip >= 0) && (skip <= data.readableBytes()));
            Preconditions.checkArgument((skip > 0) || (newLength < length));

            ByteBuf newData = data.slice(data.readerIndex() + skip, newLength);
            assert (newLength == newData.readableBytes());

            return new ByteBufChunk(newStart, newLength, newData);
        }

        @Override
        public String toString() {
            return "ByteBufChunk [start=" + start + ", length=" + length + ", data.readable=" + data.readableBytes()
                    + "]";
        }
    }

    public ListenableFuture<ByteBuf> read(int segmentOffset, final int length) {
        assert length >= 0;

        List<ListenableFuture<ByteBuf>> futures = Lists.newArrayList();

        int remaining = length;
        while (remaining > 0) {
            Chunk chunk = null;

            {
                Entry<Integer, Chunk> entry = chunks.floorEntry(segmentOffset);
                if (entry != null) {
                    chunk = entry.getValue();

                    assert chunk.start <= segmentOffset;
                    if (chunk.end() <= segmentOffset) {
                        chunk = null;
                    }
                }
            }

            int n;
            if (chunk == null) {
                Entry<Integer, Chunk> next = chunks.ceilingEntry(segmentOffset);
                if (next == null) {
                    n = remaining;
                } else {
                    n = Math.min(remaining, next.getValue().start - segmentOffset);
                }

                assert n > 0;

                addZeroes(futures, n);
            } else {
                int chunkOffset = segmentOffset - chunk.start;
                n = Math.min(chunk.length - chunkOffset, remaining);

                assert n > 0;

                ListenableFuture<ByteBuf> data = read(chunk, chunkOffset, n);
                futures.add(data);
            }

            segmentOffset += n;
            remaining -= n;
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

    // private static final ByteBuffer ZERO = ByteBuffer.allocateDirect(4096).asReadOnlyBuffer();
    private static final ByteBuf ZERO_BUF;
    static {
        ZERO_BUF = Unpooled.directBuffer(4096);
        ZERO_BUF.writerIndex(4096);
    }

    protected static final int SPACE_SEGMENTS = 0;

    private void addZeroes(List<ListenableFuture<ByteBuf>> futures, int length) {
        while (length > 0) {
            ByteBuf b = Unpooled.unmodifiableBuffer(ZERO_BUF.retain());

            int n = length;
            if (n >= b.readableBytes()) {
                n = b.readableBytes();
            } else {
                b = b.slice(b.readerIndex(), n);
            }

            assert b.readableBytes() == n;
            assert n <= length;

            assert n > 0;

            futures.add(Futures.immediateFuture(b));
            length -= n;

            assert length >= 0;
        }
    }

    private ListenableFuture<ByteBuf> read(Chunk chunk, final int offset, final int length) {
        assert offset >= 0;
        assert (offset + length) <= chunk.length;

        if (chunk instanceof BlobChunk) {
            final BlobChunk blobChunk = (BlobChunk) chunk;

            ListenableFuture<BlobCache.CacheFileHandle> buffer = cloudVolume.blobStore.find(blobChunk.hash);

            return Futures.transform(buffer, new Function<BlobCache.CacheFileHandle, ByteBuf>() {
                @Override
                public ByteBuf apply(BlobCache.CacheFileHandle handle) {
                    ByteBuffer b = handle.getData();

                    b = b.duplicate();
                    b.position(offset + blobChunk.skip);
                    b.limit(offset + blobChunk.skip + length);
                    b = b.slice();

                    ByteBuf bb = new ExternalByteBuf(Unpooled.wrappedBuffer(b), handle);
                    assert bb.refCnt() == 1;
                    return bb;
                }
            });
        } else if (chunk instanceof ByteBufChunk) {
            final ByteBufChunk byteBufChunk = (ByteBufChunk) chunk;

            ByteBuf slice = byteBufChunk.data.slice(offset, length);
            slice.retain();
            return Futures.immediateFuture(slice);
        } else {
            throw new IllegalStateException();
        }

    }

    public void write(int segmentOffset, ByteBuf data) {
        int length = data.readableBytes();
        assert length >= 0;

        final ByteBufChunk newChunk = new ByteBufChunk(segmentOffset, length, data);

        synchronized (lock) {
            List<Chunk> addList = Lists.newArrayList();
            addList.add(newChunk);

            // Find and trim any existing chunks
            // TODO: This is order(N); we could probably skip to the first overlap in logN
            Iterator<Entry<Integer, Chunk>> iterator = chunks.entrySet().iterator();
            while (iterator.hasNext()) {
                Chunk existing = iterator.next().getValue();
                if (existing.start < newChunk.start) {
                    if (existing.end() <= newChunk.start) {
                        assert !newChunk.overlaps(existing);
                        // Don't remove
                        continue;
                    } else {
                        assert newChunk.overlaps(existing);

                        {
                            Chunk slice = existing.slice(existing.start, newChunk.start - existing.start);
                            assert slice.end() == newChunk.start;
                            assert !slice.isEmpty();
                            addList.add(slice);
                        }

                        if (newChunk.end() < existing.end()) {
                            assert existing.entirelyContains(newChunk);

                            Chunk slice = existing.slice(newChunk.end());
                            assert (!slice.isEmpty());
                            assert slice.start == newChunk.end();
                            assert slice.end() == existing.end();
                            addList.add(slice);
                        }
                    }
                } else if (existing.start == newChunk.start) {
                    assert newChunk.overlaps(existing);

                    if (newChunk.end() >= existing.end()) {
                        assert newChunk.entirelyContains(existing);
                    } else {
                        Chunk slice = existing.slice(newChunk.end());
                        assert newChunk.end() == slice.start;
                        assert !slice.isEmpty();
                        addList.add(slice);
                    }
                } else if (existing.start > newChunk.start) {
                    if (existing.start >= newChunk.end()) {
                        assert !newChunk.overlaps(existing);
                        // Because of sort order, all remaining are >
                        break;
                    }

                    assert newChunk.overlaps(existing);

                    if (existing.end() <= newChunk.end()) {
                        assert newChunk.entirelyContains(existing);
                    } else {
                        Chunk slice = existing.slice(newChunk.end());
                        assert (!slice.isEmpty());
                        assert slice.start == newChunk.end();
                        assert slice.end() == existing.end();
                        addList.add(slice);
                    }
                }

                // By default we remove existing
                existing.close();
                iterator.remove();
            }

            // Actually insert the chunk and any other chunk
            for (Chunk chunk : addList) {
                chunks.put(chunk.start, chunk);
            }

            assert isValid();

            version++;
        }

        cloudVolume.executors.scheduledExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                // This should be fast, so we don't bother with the redispatch to an executor
                synchronized (lock) {
                    Chunk currentChunk = chunks.get(newChunk.start);
                    if (currentChunk != newChunk) {
                        return;
                    }
                }
                write(newChunk);
            }
        }, 1, TimeUnit.SECONDS);
    }

    private boolean isValid() {
        synchronized (lock) {
            Chunk lastChunk = null;
            for (Entry<Integer, Chunk> entry : chunks.entrySet()) {
                Chunk chunk = entry.getValue();
                if (entry.getKey().intValue() != chunk.start) {
                    return false;
                }

                if (chunk.length == 0) {
                    return false;
                }

                if (lastChunk != null) {
                    if (lastChunk.end() > chunk.start) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    // private ListenableFuture<Void> scheduleWrite(Chunk newChunk) {
    // final long start = newChunk.start;
    // final long end = newChunk.end;
    //
    // cloudVolume.executor.submit(new Callable<Void>() {
    // @Override
    // public Void call() throws Exception {
    // Chunk chunk = chunks.get(start);
    //
    // if (chunk != newChunk) {
    //
    // }
    // ByteSource source = new ByteBufByteSource(slice);
    //
    // ByteString prefix = ByteString.EMPTY;
    //
    // BlobStore.WriteOperation writeOp = blobStore.put(prefix, source);
    //
    // ByteString key = writeOp.getKey().get();
    //
    // assert prefix.size() == 0;
    // ByteString hash = key;
    //
    // }
    //
    // });
    //
    // }

    private ListenableFuture<ByteString> write(final ByteBufChunk chunk) {
        synchronized (lock) {
            ListenableFuture<ByteString> writeOperation = writeOperations.get(chunk);
            if (writeOperation != null) {
                // Join existing write operation
                return writeOperation;
            }

            assert !chunk.isClosed();

            ByteBuf data = chunk.data;

            ByteSource source = new ByteBufByteSource(data);
            final ByteString prefix = cloudVolume.getBlobPrefix();
            writeOperation = cloudVolume.blobStore.put(prefix, source);
            writeOperations.put(chunk, writeOperation);

            Futures.addCallback(writeOperation, new FutureCallback<ByteString>() {

                @Override
                public void onSuccess(ByteString hash) {
                    synchronized (lock) {
                        int start = chunk.start;
                        int length = chunk.length;

                        int skip = 0;

                        Chunk currentChunk = chunks.get(start);

                        if (chunk == currentChunk) {
                            BlobChunk blobChunk = new BlobChunk(start, length, hash, skip);
                            chunks.put(start, blobChunk);
                        }

                        writeOperations.remove(chunk);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    log.warn("Failed blob write: " + chunk, t);

                    synchronized (lock) {
                        writeOperations.remove(chunk);
                    }
                }
            });

            return writeOperation;
        }
    }

    ListenableFuture<Void> sync() {
        // final SettableFuture<Void> syncFuture = SettableFuture.create();

        synchronized (lock) {
            try {
                if (version <= wroteVersion) {
                    assert version == wroteVersion;
                    return Futures.immediateFuture(null);
                }

                final long snapshotVersion = version;

                ListenableFuture<Void> existingOperation = syncOperations.get(snapshotVersion);
                if (existingOperation != null) {
                    // Join existing sync operation
                    return existingOperation;
                }

                final TreeMap<Integer, Chunk> snapshot = Maps.newTreeMap(chunks);

                List<ListenableFuture<ByteString>> futures = Lists.newArrayList();
                final List<ChunkData.Builder> futureChunks = Lists.newArrayList();

                final SegmentData.Builder sb = SegmentData.newBuilder();

                {
                    for (Chunk chunk : snapshot.values()) {
                        final ChunkData.Builder cb = sb.addChunkBuilder();
                        cb.setStart(chunk.start);
                        cb.setLength(chunk.length);

                        if (chunk instanceof BlobChunk) {
                            BlobChunk blobChunk = (BlobChunk) chunk;

                            assert blobChunk.hash != null;

                            cb.setHash(blobChunk.hash);

                            cb.setSkip(blobChunk.skip);
                        } else if (chunk instanceof ByteBufChunk) {
                            ByteBufChunk byteBufChunk = (ByteBufChunk) chunk;

                            ListenableFuture<ByteString> storeFuture = write(byteBufChunk);

                            futures.add(storeFuture);
                            futureChunks.add(cb);
                        } else {
                            throw new IllegalStateException();
                        }
                    }

                    sb.setOffset(volumeOffset);
                }

                ListenableFuture<List<ByteString>> writeBlobs = Futures.allAsList(futures);
                ListenableFuture<SegmentData> segmentData = Futures.transform(writeBlobs,
                        new Function<List<ByteString>, SegmentData>() {

                            @Override
                            public SegmentData apply(List<ByteString> blobHashes) {
                                assert blobHashes.size() == futureChunks.size();

                                for (int i = 0; i < blobHashes.size(); i++) {
                                    ByteString hash = blobHashes.get(i);

                                    ChunkData.Builder cb = futureChunks.get(i);
                                    cb.setHash(hash);

                                    assert cb.getSkip() == 0;
                                }

                                SegmentData segmentData = sb.build();
                                return segmentData;
                            }
                        });

                final AtomicReference<ByteString> wroteKeyValueVersion = new AtomicReference<ByteString>();
                ListenableFuture<Boolean> writeSegmentData = Futures.transform(segmentData,
                        new AsyncFunction<SegmentData, Boolean>() {

                            @Override
                            public ListenableFuture<Boolean> apply(SegmentData segmentData) throws Exception {
                                synchronized (lock) {
                                    // TODO: This logic is complicated, and I'm not entirely sure it is correct
                                    // I think the Compare-and-swap is sufficient to ensure progress
                                    // We have some false-failures, but that is OK; we reuse most of the work
                                    if (wroteVersion >= snapshotVersion) {
                                        log.warn("Concurrent sync operation; skipping write");
                                        return Futures.immediateFuture(Boolean.FALSE);
                                    }

                                    // TODO: Use compare-and-swap to protect against any separate modification

                                    Modifier modifier;
                                    if (currentKeyValueVersion == null) {
                                        modifier = new IfNotExists();
                                    } else {
                                        modifier = new IfVersion(currentKeyValueVersion);
                                    }
                                    ByteString newKeyValueVersion = segmentData.toByteString();
                                    wroteKeyValueVersion.set(newKeyValueVersion);

                                    ListenableFuture<Boolean> f = cloudVolume.keyValueStore.putAsync(SPACE_SEGMENTS,
                                            segmentKey, newKeyValueVersion, modifier);

                                    return f;
                                }
                            }
                        });

                ListenableFuture<Void> finalResult = Futures.transform(writeSegmentData, new Function<Boolean, Void>() {
                    @Override
                    public Void apply(Boolean success) {
                        synchronized (lock) {
                            if (Boolean.TRUE.equals(success)) {
                                if (wroteVersion < snapshotVersion) {
                                    wroteVersion = snapshotVersion;
                                    currentKeyValueVersion = wroteKeyValueVersion.get();
                                    assert wroteKeyValueVersion.get() != null;
                                    return null;
                                } else {
                                    log.error("Failed sync: wrote old version.  Segment={}", Segment.this);
                                    throw new IllegalStateException();
                                }
                            } else {
                                log.warn("Failed sync; likely concurrent sync operation.  Segment={}", Segment.this);
                                throw new IllegalStateException();
                            }
                        }
                    }
                });

                syncOperations.put(snapshotVersion, finalResult);

                Futures.addCallback(finalResult, new FutureCallback<Void>() {
                    private void cleanup() {
                        synchronized (lock) {
                            syncOperations.remove(snapshotVersion);
                        }
                    }

                    @Override
                    public void onSuccess(Void result) {
                        cleanup();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        cleanup();
                    }
                });

                return finalResult;
            } catch (Throwable t) {
                return Futures.immediateFailedFuture(t);
            }
        }
    }

    public boolean isDirty() {
        synchronized (lock) {
            assert wroteVersion <= version;
            return wroteVersion != version;
        }
    }
}
