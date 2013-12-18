package com.cloudata.structured;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.Btree;
import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.EntryListener;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.MmapPageStore;
import com.cloudata.btree.PageStore;
import com.cloudata.btree.ReadOnlyTransaction;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.btree.operation.SetOperation;
import com.cloudata.structured.operation.StructuredOperation;
import com.cloudata.values.Value;
import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;

public class StructuredStore {

    private static final Logger log = LoggerFactory.getLogger(StructuredStore.class);

    final Btree btree;

    static final Keyspace NAME_TO_ID = Keyspace.system(1);
    static final Keyspace ID_TO_NAME = Keyspace.system(2);

    public StructuredStore(File dir, boolean uniqueKeys) throws IOException {
        File data = new File(dir, "data");
        PageStore pageStore = MmapPageStore.build(data, uniqueKeys);

        log.warn("Building new btree @{}", dir);

        this.btree = new Btree(pageStore, uniqueKeys);
    }

    public void doAction(ByteBuffer key, StructuredOperation<?> operation) {
        try (WriteTransaction txn = btree.beginReadWrite()) {
            txn.doAction(btree, key, operation);
            txn.commit();
        }
    }

    public Value get(final ByteBuffer key) {
        try (ReadOnlyTransaction txn = btree.beginReadOnly()) {
            return txn.get(btree, key);
        }
    }

    public BtreeQuery buildQuery(Keyspace keyspace) {
        return new BtreeQuery(btree, keyspace);
    }

    static class FindMaxListener implements EntryListener {

        private ByteBuffer lastKey;

        public ByteBuffer getLastKey() {
            return lastKey;
        }

        @Override
        public boolean found(ByteBuffer key, Value value) {
            if (ID_TO_NAME.contains(key)) {
                this.lastKey = key;
                return true;
            } else {
                return false;
            }
        }

    }

    public void ensureKeys(WriteTransaction txn, Set<String> keys) {
        // TODO: Cache

        long nextId = -1;

        for (String s : keys) {
            ByteString key = NAME_TO_ID.mapToKey(ByteString.copyFromUtf8(s));
            Value existing = txn.get(btree, key.asReadOnlyByteBuffer());
            if (existing == null) {
                if (nextId < 0) {
                    log.warn("Find max nextId logic is stupid");
                    FindMaxListener listener = new FindMaxListener();
                    txn.walk(btree, ID_TO_NAME.mapToKey(ByteString.EMPTY).asReadOnlyByteBuffer(), listener);
                    ByteBuffer lastKey = listener.getLastKey();
                    if (lastKey == null || !ID_TO_NAME.contains(lastKey)) {
                        nextId = 1;
                    } else {
                        ByteBuffer suffix = ID_TO_NAME.getSuffix(lastKey);
                        long lastId = suffix.getLong();
                        nextId = lastId + 1;
                    }
                }

                assert nextId > 0;

                long id = nextId++;

                Value idValue = Value.fromLong(id);
                txn.doAction(btree, key.asReadOnlyByteBuffer(), new SetOperation(idValue));

                Value stringValue = Value.fromRawBytes(s.getBytes(Charsets.UTF_8));
                txn.doAction(btree, ID_TO_NAME.mapToKey(encode(id)).asReadOnlyByteBuffer(), new SetOperation(
                        stringValue));
            }
        }
    }

    private static byte[] encode(long id) {
        byte[] data = new byte[8];
        ByteBuffer.wrap(data).putLong(id);
        return data;
    }

    public Btree getBtree() {
        return btree;
    }

}
