package com.cloudata.structured;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.Keyspace;
import com.cloudata.structured.StructuredProto.LogEntry;
import com.cloudata.structured.operation.DeleteOperation;
import com.cloudata.structured.operation.SetOperation;
import com.cloudata.structured.operation.StructuredOperation;
import com.cloudata.values.Value;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class StructuredStateMachine implements StateMachine {
    private static final Logger log = LoggerFactory.getLogger(StructuredStateMachine.class);

    private RaftService raft;
    private File baseDir;
    final LoadingCache<Long, StructuredStore> keyValueStoreCache;

    public StructuredStateMachine() {
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

    public <V> V doAction(long storeId, Keyspace keyspace, ByteString key, StructuredOperation<V> operation)
            throws InterruptedException, RaftException {
        LogEntry.Builder entry = operation.serialize();
        entry.setKey(keyspace.mapToKey(key));
        entry.setStoreId(storeId);

        log.debug("Proposing operation {}", entry.getAction());

        return (V) raft.commit(entry.build().toByteArray());
    }

    @Override
    public Object applyOperation(@Nonnull ByteBuffer op) {
        // TODO: We need to prevent repetition during replay
        // (we need idempotency)
        try {
            LogEntry entry = LogEntry.parseFrom(ByteString.copyFrom(op));
            log.debug("Committing operation {}", entry.getAction());

            long storeId = entry.getStoreId();

            ByteString key = entry.getKey();
            ByteString value = entry.getValue();

            StructuredStore keyValueStore = getKeyValueStore(storeId);

            StructuredOperation<?> operation;

            switch (entry.getAction()) {

            case DELETE:
                operation = new DeleteOperation();
                break;

            case SET:
                operation = new SetOperation(Value.deserialize(entry.getValue().asReadOnlyByteBuffer()));
                break;

            default:
                throw new UnsupportedOperationException();
            }

            keyValueStore.doAction(key != null ? key.asReadOnlyByteBuffer() : null, operation);

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

    private StructuredStore getKeyValueStore(long id) {
        try {
            return keyValueStoreCache.get(id);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Immutable
    final class KeyValueStoreCacheLoader extends CacheLoader<Long, StructuredStore> {

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

    public Value get(long storeId, Keyspace keyspace, ByteString key) {
        StructuredStore keyValueStore = getKeyValueStore(storeId);
        return keyValueStore.get(keyspace.mapToKey(key).asReadOnlyByteBuffer());
    }

    public BtreeQuery scan(long storeId) {
        StructuredStore keyValueStore = getKeyValueStore(storeId);
        return keyValueStore.buildQuery();
    }

}
