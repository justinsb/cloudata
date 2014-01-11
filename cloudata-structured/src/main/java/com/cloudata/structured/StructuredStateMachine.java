package com.cloudata.structured;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.robotninjas.barge.NoLeaderException;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.Keyspace;
import com.cloudata.structured.StructuredProtocol.StructuredAction;
import com.cloudata.structured.StructuredProtocol.StructuredActionResponse;
import com.cloudata.structured.operation.StructuredOperation;
import com.cloudata.structured.operation.StructuredOperations;
import com.cloudata.values.Value;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class StructuredStateMachine implements StateMachine {
    private static final Logger log = LoggerFactory.getLogger(StructuredStateMachine.class);

    private RaftService raft;
    private File baseDir;
    final LoadingCache<Long, StructuredStore> storeCache;

    private final ListeningExecutorService executor;

    public StructuredStateMachine(ListeningExecutorService executor) {
        this.executor = executor;

        StoreCacheLoader loader = new StoreCacheLoader();
        this.storeCache = CacheBuilder.newBuilder().recordStats().build(loader);
    }

    public void init(RaftService raft, File stateDir) {
        this.raft = raft;
        this.baseDir = stateDir;
    }

    // public LogMessageIterable readLog(long logId, long begin, int max) {
    // LogFileSet logFileSet = getKeyValueStore(logId);
    // return logFileSet.readLog(logId, begin, max);
    // }

    // public Object put(long storeId, byte[] key, byte[] value) throws InterruptedException, RaftException {
    // KvEntry entry = KvEntry.newBuilder().setStoreId(storeId).setKey(ByteString.copyFrom(key))
    // .setAction(KvAction.SET).setValue(ByteString.copyFrom(value)).build();
    //
    // log.debug("Proposing operation {}", entry.getAction());
    //
    // return raft.commit(entry.toByteArray());
    // }

    public StructuredActionResponse doActionSync(StructuredOperation operation) throws InterruptedException,
            RaftException {
        try {
            return doActionAsync(operation).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof NotLeaderException) {
                throw (NotLeaderException) cause;
            }
            if (cause instanceof NoLeaderException) {
                throw (NoLeaderException) cause;
            }
            throw new RaftException(e.getCause());
        }
    }

    public ListenableFuture<StructuredActionResponse> doActionAsync(final StructuredOperation operation)
            throws RaftException {
        if (operation.isReadOnly()) {
            return executor.submit(new Callable<StructuredActionResponse>() {

                @Override
                public StructuredActionResponse call() throws Exception {
                    // TODO: Need to check that we are the leader!!

                    long storeId = operation.getStoreId();

                    StructuredStore store = getStructuredStore(storeId);

                    store.doAction(operation);

                    return operation.getResult();
                }
            });
        }

        StructuredAction entry = operation.serialize();

        log.debug("Proposing operation {}", entry.getAction());

        return Futures.transform(raft.commitAsync(entry.toByteArray()),
                new Function<Object, StructuredActionResponse>() {
                    @Override
                    public StructuredActionResponse apply(Object input) {
                        Preconditions.checkArgument(input instanceof StructuredActionResponse);
                        return (StructuredActionResponse) input;
                    }
                });
    }

    // public <V> V doAction(long storeId, StructuredOperation<V> operation) throws InterruptedException, RaftException
    // {
    // LogEntry.Builder entry = operation.serialize();
    // entry.setStoreId(storeId);
    //
    // log.debug("Proposing operation {}", entry.getAction());
    //
    // return (V) raft.commit(entry.build().toByteArray());
    // }

    @Override
    public Object applyOperation(@Nonnull ByteBuffer op) {
        // TODO: We need to prevent repetition during replay
        // (we need idempotency)
        try {
            StructuredAction action = StructuredAction.parseFrom(ByteString.copyFrom(op));
            log.debug("Committing operation {}", action.getAction());

            long storeId = action.getStoreId();

            StructuredStore store = getStructuredStore(storeId);

            StructuredOperation operation = StructuredOperations.build(store, action);

            store.doAction(operation);

            Object ret = operation.getResult();
            return ret;
        } catch (InvalidProtocolBufferException e) {
            log.error("Error deserializing operation", e);
            throw new IllegalArgumentException("Error deserializing key value operation", e);
        } catch (Throwable e) {
            log.error("Error performing key value operation", e);
            throw new IllegalArgumentException("Error performing key value operation", e);
        }
    }

    public StructuredStore getStructuredStore(long id) {
        try {
            return storeCache.get(id);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Immutable
    final class StoreCacheLoader extends CacheLoader<Long, StructuredStore> {

        @Override
        public StructuredStore load(@Nonnull Long id) throws Exception {
            checkNotNull(id);
            try {
                log.debug("Loading KeyValueStore {}", id);
                File dir = new File(baseDir, Long.toHexString(id));

                dir.mkdirs();

                boolean uniqueKeys = true;
                return new StructuredStore(dir, uniqueKeys);
            } catch (Exception e) {
                log.warn("Error building KeyValueStore", e);
                throw e;
            }
        }
    }

    // This should also really be an operation, but is special-cased for speed
    public Value get(StructuredStore store, Keyspace keyspace, ByteString key) {
        ByteString qualifiedKey = keyspace.mapToKey(key);
        return store.get(qualifiedKey.asReadOnlyByteBuffer());
    }

    // This should also really be an operation, but is special-cased for speed
    public BtreeQuery scan(StructuredStore store, Keyspace keyspace) {
        return store.buildQuery(keyspace, true);
    }

    // This should also really be an operation, but is special-cased for speed
    public void listKeys(StructuredStore store, Keyspace keyspace, Listener<ByteBuffer> listener) {
        if (keyspace != null) {
            store.getKeys().listKeys(keyspace, listener);
        }

        listener.done();
    }

}
