package com.cloudata.structured;

import java.nio.ByteBuffer;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.Btree;
import com.cloudata.btree.EntryListener;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.Transaction;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.btree.operation.SimpleSetOperation;
import com.cloudata.structured.StructuredStore.FindMaxListener;
import com.cloudata.util.ByteStrings;
import com.cloudata.values.Value;
import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;

public class Keys {
    private static final Logger log = LoggerFactory.getLogger(Keys.class);

    static final Keyspace NAME_TO_ID = Keyspace.system(1);
    static final Keyspace ID_TO_NAME = Keyspace.system(2);

    final Btree btree;

    public Keys(Btree btree) {
        this.btree = btree;
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
                txn.doAction(btree, new SimpleSetOperation(ID_TO_NAME.mapToKey(ByteStrings.encode(id)), stringValue));
            }
        }
    }

}
