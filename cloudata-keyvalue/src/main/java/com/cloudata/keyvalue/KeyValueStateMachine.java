package com.cloudata.keyvalue;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
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
import com.cloudata.keyvalue.KeyValueProtocol.ActionResponse;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueAction;
import com.cloudata.keyvalue.operation.KeyValueOperation;
import com.cloudata.keyvalue.operation.KeyValueOperations;
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

public class KeyValueStateMachine implements StateMachine, Closeable {
    private static final Logger log = LoggerFactory.getLogger(KeyValueStateMachine.class);

    private RaftService raft;
    private final File baseDir;
    final LoadingCache<Long, KeyValueStore> keyValueStoreCache;

    private final ListeningExecutorService executor;

    public KeyValueStateMachine(ListeningExecutorService executor, File baseDir) {
        this.executor = executor;
        this.baseDir = baseDir;
        KeyValueStoreCacheLoader loader = new KeyValueStoreCacheLoader();
        this.keyValueStoreCache = CacheBuilder.newBuilder().recordStats().removalListener(CloseOnRemoval.build())
                .build(loader);
    }

    public void init(RaftService raft) {
        this.raft = raft;
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

    public ActionResponse doActionSync(KeyValueOperation operation) throws InterruptedException, RaftException {
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

    public ListenableFuture<ActionResponse> doActionAsync(final KeyValueOperation operation) throws RaftException {
        if (operation.isReadOnly()) {
            return executor.submit(new Callable<ActionResponse>() {

                @Override
                public ActionResponse call() throws Exception {
                    // TODO: Need to check that we are the leader!!

                    long storeId = operation.getStoreId();

                    KeyValueStore keyValueStore = getKeyValueStore(storeId);

                    keyValueStore.doAction(operation);

                    return operation.getResult();
                }
            });
        }

        KeyValueAction entry = operation.serialize();

        log.debug("Proposing operation {}", entry.getAction());

        return Futures.transform(raft.commitAsync(entry.toByteArray()), new Function<Object, ActionResponse>() {

            @Override
            public ActionResponse apply(Object input) {
                Preconditions.checkArgument(input instanceof ActionResponse);
                return (ActionResponse) input;
            }

        });
    }

    @Override
    public Object applyOperation(@Nonnull ByteBuffer op) {
        // TODO: We need to prevent repetition during replay
        // (we need idempotency)
        try {
            KeyValueAction entry = KeyValueAction.parseFrom(ByteString.copyFrom(op));
            log.debug("Committing operation {}", entry.getAction());

            long storeId = entry.getStoreId();

            KeyValueStore keyValueStore = getKeyValueStore(storeId);

            KeyValueOperation operation = KeyValueOperations.build(entry);

            keyValueStore.doAction(operation);

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

    private KeyValueStore getKeyValueStore(long id) {
        try {
            return keyValueStoreCache.get(id);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Immutable
    final class KeyValueStoreCacheLoader extends CacheLoader<Long, KeyValueStore> {

        @Override
        public KeyValueStore load(@Nonnull Long id) throws Exception {
            checkNotNull(id);
            try {
                log.debug("Loading KeyValueStore {}", id);
                File dir = new File(baseDir, Long.toHexString(id));

                dir.mkdirs();

                boolean uniqueKeys = true;
                return new KeyValueStore(dir, uniqueKeys);
            } catch (Exception e) {
                log.warn("Error building KeyValueStore", e);
                throw e;
            }
        }
    }

    // This should also really be an operation, but is special-cased for speed
    public Value get(long storeId, Keyspace keyspace, ByteString key) {
        KeyValueStore keyValueStore = getKeyValueStore(storeId);
        return keyValueStore.get(keyspace.mapToKey(key).asReadOnlyByteBuffer());
    }

    // This function should really be an operation, but we want to support streaming
    public BtreeQuery scan(long storeId, Keyspace keyspace, ByteString keyPrefix) {
        KeyValueStore keyValueStore = getKeyValueStore(storeId);
        boolean stripKeyspace = true;
        return keyValueStore.buildQuery(keyspace, stripKeyspace, keyPrefix);
    }

    @Override
    public void close() {
        keyValueStoreCache.invalidateAll();
    }

}
