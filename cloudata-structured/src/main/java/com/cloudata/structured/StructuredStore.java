package com.cloudata.structured;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.Btree;
import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.Database;
import com.cloudata.btree.EntryListener;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.ReadOnlyTransaction;
import com.cloudata.btree.Transaction;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.btree.operation.SimpleSetOperation;
import com.cloudata.structured.StructuredProto.KeyspaceData;
import com.cloudata.structured.operation.StructuredOperation;
import com.cloudata.values.Value;
import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

public class StructuredStore {

    private static final Logger log = LoggerFactory.getLogger(StructuredStore.class);

    final Btree btree;

    static final Keyspace NAME_TO_ID = Keyspace.system(1);
    static final Keyspace ID_TO_NAME = Keyspace.system(2);

    static final Keyspace KEYSPACE_NAME_TO_ID = Keyspace.system(3);
    static final Keyspace KEYSPACE_ID_TO_NAME = Keyspace.system(4);

    public StructuredStore(File dir, boolean uniqueKeys) throws IOException {
        File data = new File(dir, "data");
        Database db = Database.build(data, null);

        log.warn("Building new btree @{}", dir);

        this.btree = new Btree(db, uniqueKeys);
    }

    public void doAction(StructuredOperation<?> operation) throws IOException {
        try (WriteTransaction txn = btree.beginReadWrite()) {
            txn.doAction(btree, operation);
            txn.commit();
        }
    }

    public Value get(final ByteBuffer key) {
        try (ReadOnlyTransaction txn = btree.beginReadOnly()) {
            return txn.get(btree, key);
        }
    }

    public BtreeQuery buildQuery(Keyspace keyspace, boolean stripKeyspace) {
        return new BtreeQuery(btree, keyspace, stripKeyspace);
    }

    static class FindMaxListener implements EntryListener {

        final Keyspace keyspace;

        public FindMaxListener(Keyspace keyspace) {
            this.keyspace = keyspace;
        }

        private ByteBuffer lastKey;

        public ByteBuffer getLastKey() {
            return lastKey;
        }

        @Override
        public boolean found(ByteBuffer key, Value value) {
            if (keyspace.contains(key)) {
                this.lastKey = key;
                return true;
            } else {
                return false;
            }
        }

    }

    public void listKeys(final Keyspace keyspace, final Listener<ByteBuffer> listener) {
        try (Transaction txn = btree.beginReadOnly()) {
            txn.walk(btree, NAME_TO_ID.mapToKey(keyspace.mapToKey(ByteString.EMPTY)).asReadOnlyByteBuffer(),
                    new EntryListener() {
                        @Override
                        public boolean found(ByteBuffer key, Value value) {
                            if (!NAME_TO_ID.contains(key)) {
                                return false;
                            }

                            ByteBuffer suffix = NAME_TO_ID.getSuffix(key);
                            if (!keyspace.contains(suffix)) {
                                return false;
                            }

                            ByteBuffer name = keyspace.getSuffix(suffix);
                            return listener.next(name);
                        }
                    });
        }
    }

    public void ensureKeys(WriteTransaction txn, Keyspace keyspace, Set<String> keys) {
        // TODO: Cache

        long nextId = -1;

        for (String s : keys) {
            ByteString key = NAME_TO_ID.mapToKey(keyspace.mapToKey(ByteString.copyFromUtf8(s)));
            Value existing = txn.get(btree, key.asReadOnlyByteBuffer());
            if (existing == null) {
                if (nextId < 0) {
                    log.warn("Find max nextId logic is stupid");
                    FindMaxListener listener = new FindMaxListener(ID_TO_NAME);
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
                txn.doAction(btree, new SimpleSetOperation(key, idValue));

                Value stringValue = Value.fromRawBytes(s.getBytes(Charsets.UTF_8));
                txn.doAction(btree, new SimpleSetOperation(ID_TO_NAME.mapToKey(encode(id)), stringValue));
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

    public Keyspace findKeyspace(ByteString keyspaceName) {
        try (Transaction txn = btree.beginReadOnly()) {
            return findKeyspace(txn, keyspaceName);
        }
    }

    public Keyspace findKeyspace(Transaction txn, ByteString keyspaceName) {
        ByteString key = KEYSPACE_NAME_TO_ID.mapToKey(keyspaceName);
        Value existing = txn.get(btree, key.asReadOnlyByteBuffer());
        if (existing == null) {
            return null;
        }
        return Keyspace.user(Ints.checkedCast(existing.asLong()));
    }

    public Keyspace ensureKeyspace(WriteTransaction txn, ByteString keyspaceName) {
        // TODO: Cache

        long nextId = -1;

        ByteString key = KEYSPACE_NAME_TO_ID.mapToKey(keyspaceName);
        Value existing = txn.get(btree, key.asReadOnlyByteBuffer());
        if (existing == null) {
            if (nextId < 0) {
                log.warn("Find max nextId logic is stupid");
                FindMaxListener listener = new FindMaxListener(KEYSPACE_ID_TO_NAME);
                txn.walk(btree, KEYSPACE_ID_TO_NAME.mapToKey(ByteString.EMPTY).asReadOnlyByteBuffer(), listener);
                ByteBuffer lastKey = listener.getLastKey();
                if (lastKey == null || !KEYSPACE_ID_TO_NAME.contains(lastKey)) {
                    nextId = 1;
                } else {
                    ByteBuffer suffix = KEYSPACE_ID_TO_NAME.getSuffix(lastKey);
                    long lastId = suffix.getLong();
                    nextId = lastId + 1;
                }
            }

            assert nextId > 0;

            long id = nextId++;

            Value idValue = Value.fromLong(id);
            txn.doAction(btree, new SimpleSetOperation(key, idValue));

            KeyspaceData.Builder b = KeyspaceData.newBuilder();
            b.setId(id);
            b.setName(keyspaceName);
            Value stringValue = Value.fromRawBytes(b.build().toByteString());
            txn.doAction(btree, new SimpleSetOperation(KEYSPACE_ID_TO_NAME.mapToKey(encode(id)), stringValue));

            return Keyspace.user(Ints.checkedCast(id));
        } else {
            return Keyspace.user(Ints.checkedCast(existing.asLong()));
        }
    }

}
