//package com.cloudata.blockstore;
//
//import static com.google.common.base.Preconditions.checkNotNull;
//
//import java.io.File;
//import java.nio.ByteBuffer;
//import java.util.concurrent.ExecutionException;
//
//import javax.annotation.Nonnull;
//import javax.annotation.concurrent.Immutable;
//
//import org.robotninjas.barge.RaftException;
//import org.robotninjas.barge.RaftService;
//import org.robotninjas.barge.StateMachine;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.cloudata.blockstore.IscsiProto.LogEntry;
//import com.cloudata.blockstore.operation.BlockStoreOperation;
//import com.cloudata.btree.BtreeQuery;
//import com.cloudata.btree.Keyspace;
//import com.cloudata.clients.keyvalue.KeyValueStore;
//import com.cloudata.values.Value;
//import com.google.common.base.Throwables;
//import com.google.common.cache.CacheBuilder;
//import com.google.common.cache.CacheLoader;
//import com.google.common.cache.LoadingCache;
//import com.google.protobuf.ByteString;
//import com.google.protobuf.InvalidProtocolBufferException;
//
//public class BlockStoreStateMachine implements StateMachine {
//    private static final Logger log = LoggerFactory.getLogger(BlockStoreStateMachine.class);
//
//    private RaftService raft;
//    private File baseDir;
//    final LoadingCache<Long, KeyValueStore> keyValueStoreCache;
//
//    public BlockStoreStateMachine() {
//        KeyValueStoreCacheLoader loader = new KeyValueStoreCacheLoader();
//        this.keyValueStoreCache = CacheBuilder.newBuilder().recordStats().build(loader);
//    }
//
//    public void init(RaftService raft, File stateDir) {
//        this.raft = raft;
//        this.baseDir = stateDir;
//    }
//
//    public <V> V doAction(long storeId, BlockStoreOperation<V> operation) throws InterruptedException, RaftException {
//        LogEntry.Builder entry = operation.serialize();
//        entry.setStoreId(storeId);
//
//        log.debug("Proposing operation {}", entry.getAction());
//
//        return (V) raft.commit(entry.build().toByteArray());
//    }
//
//    @Override
//    public Object applyOperation(@Nonnull ByteBuffer op) {
//        // TODO: We need to prevent repetition during replay
//        // (we need idempotency)
//        try {
//            LogEntry entry = LogEntry.parseFrom(ByteString.copyFrom(op));
//            log.debug("Committing operation {}", entry.getAction());
//
//            long storeId = entry.getStoreId();
//
//            ByteString key = entry.getKey();
//            ByteString value = entry.getValue();
//
//            KeyValueStore keyValueStore = getKeyValueStore(storeId);
//
//            BlockStoreOperation<?> operation;
//
//            switch (entry.getAction()) {
//
//            // case APPEND:
//            // operation = new AppendOperation(key, value);
//            // break;
//            //
//            // case DELETE:
//            // operation = new DeleteOperation(key);
//            // break;
//            //
//            // case INCREMENT: {
//            // long delta = 1;
//            // if (entry.hasIncrementBy()) {
//            // delta = entry.getIncrementBy();
//            // }
//            // operation = new IncrementOperation(key, delta);
//            // break;
//            // }
//            //
//            // case SET:
//            // operation = new SetOperation(key, Value.deserialize(entry.getValue().asReadOnlyByteBuffer()));
//            // break;
//
//            default:
//                throw new UnsupportedOperationException();
//            }
//
//            // keyValueStore.doAction(operation);
//            //
//            // Object ret = operation.getResult();
//            //
//            // return ret;
//        } catch (InvalidProtocolBufferException e) {
//            log.error("Error deserializing operation", e);
//            throw new IllegalArgumentException("Error deserializing key value operation", e);
//        } catch (Throwable e) {
//            log.error("Error performing key value operation", e);
//            throw new IllegalArgumentException("Error performing key value operation", e);
//        }
//    }
//
//    private KeyValueStore getKeyValueStore(long id) {
//        try {
//            return keyValueStoreCache.get(id);
//        } catch (ExecutionException e) {
//            throw Throwables.propagate(e);
//        }
//    }
//
//    @Immutable
//    final class KeyValueStoreCacheLoader extends CacheLoader<Long, KeyValueStore> {
//
//        @Override
//        public KeyValueStore load(@Nonnull Long id) throws Exception {
//            checkNotNull(id);
//            try {
//                log.debug("Loading KeyValueStore {}", id);
//                File dir = new File(baseDir, Long.toHexString(id));
//
//                dir.mkdirs();
//
//                boolean uniqueKeys = true;
//                return new KeyValueStore(dir, uniqueKeys);
//            } catch (Exception e) {
//                log.warn("Error building KeyValueStore", e);
//                throw e;
//            }
//        }
//    }
//
//    public Value get(long storeId, Keyspace keyspace, ByteString key) {
//        KeyValueStore keyValueStore = getKeyValueStore(storeId);
//        return keyValueStore.get(keyspace.mapToKey(key).asReadOnlyByteBuffer());
//    }
//
//    public BtreeQuery scan(long storeId, Keyspace keyspace) {
//        KeyValueStore keyValueStore = getKeyValueStore(storeId);
//        return keyValueStore.buildQuery(keyspace, true);
//    }
//
// }
