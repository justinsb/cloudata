package com.cloudata.keyvalue;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.nio.ByteBuffer;
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
import com.cloudata.keyvalue.KeyValueLog.KvEntry;
import com.cloudata.keyvalue.operation.AppendOperation;
import com.cloudata.keyvalue.operation.DeleteOperation;
import com.cloudata.keyvalue.operation.IncrementOperation;
import com.cloudata.keyvalue.operation.KeyOperation;
import com.cloudata.keyvalue.operation.SetOperation;
import com.cloudata.values.Value;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class KeyValueStateMachine implements StateMachine {
    private static final Logger log = LoggerFactory.getLogger(KeyValueStateMachine.class);

    private RaftService raft;
    private File baseDir;
    final LoadingCache<Long, KeyValueStore> keyValueStoreCache;

    public KeyValueStateMachine() {
        KeyValueStoreCacheLoader loader = new KeyValueStoreCacheLoader();
        this.keyValueStoreCache = CacheBuilder.newBuilder().recordStats().build(loader);
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

    public <V> V doActionSync(KeyOperation<V> operation) throws InterruptedException, RaftException {
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

    public <V> ListenableFuture<V> doActionAsync(KeyOperation<V> operation) throws RaftException {
        KvEntry entry = operation.serialize();

        log.debug("Proposing operation {}", entry.getAction());

        return (ListenableFuture<V>) raft.commitAsync(entry.toByteArray());
    }

    @Override
    public Object applyOperation(@Nonnull ByteBuffer op) {
        // TODO: We need to prevent repetition during replay
        // (we need idempotency)
        try {
            KvEntry entry = KvEntry.parseFrom(ByteString.copyFrom(op));
            log.debug("Committing operation {}", entry.getAction());

            long storeId = entry.getStoreId();

            KeyValueStore keyValueStore = getKeyValueStore(storeId);

            KeyOperation<?> operation;

            switch (entry.getAction()) {

            case APPEND:
                operation = new AppendOperation(entry);
                break;

            case DELETE:
                operation = new DeleteOperation(entry);
                break;

            case INCREMENT: {
                operation = new IncrementOperation(entry);
                break;
            }

            case SET:
                operation = new SetOperation(entry);
                break;

            default:
                throw new UnsupportedOperationException();
            }

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

    public Value get(long storeId, Keyspace keyspace, ByteString key) {
        KeyValueStore keyValueStore = getKeyValueStore(storeId);
        return keyValueStore.get(keyspace.mapToKey(key).asReadOnlyByteBuffer());
    }

    public BtreeQuery scan(long storeId, Keyspace keyspace, ByteString keyPrefix) {
        KeyValueStore keyValueStore = getKeyValueStore(storeId);
        boolean stripKeyspace = true;
        return keyValueStore.buildQuery(keyspace, stripKeyspace, keyPrefix);
    }

}
