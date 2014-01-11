package com.cloudata.structured;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.Btree;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.Transaction;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.btree.operation.SimpleSetOperation;
import com.cloudata.structured.StructuredProtocol.KeyspaceData;
import com.cloudata.structured.StructuredProtocol.KeyspaceName;
import com.cloudata.structured.StructuredProtocol.KeyspaceType;
import com.cloudata.structured.StructuredStore.FindMaxListener;
import com.cloudata.util.ByteStrings;
import com.cloudata.values.Value;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

public class Keyspaces {
    private static final Logger log = LoggerFactory.getLogger(Keyspaces.class);

    final Btree btree;

    static final Keyspace KEYSPACE_NAME_TO_ID = Keyspace.system(3);
    static final Keyspace KEYSPACE_ID_TO_NAME = Keyspace.system(4);

    public Keyspaces(Btree btree) {
        this.btree = btree;
    }

    public Keyspace findKeyspace(String keyspaceName) {
        try (Transaction txn = btree.beginReadOnly()) {
            return findKeyspace(txn, keyspaceName);
        }
    }

    public Keyspace findKeyspace(Transaction txn, String keyspaceName) {
        KeyspaceName.Builder b = KeyspaceName.newBuilder();
        b.setType(KeyspaceType.USER_DATA);
        b.setName(ByteString.copyFromUtf8(keyspaceName));

        ByteString key = KEYSPACE_NAME_TO_ID.mapToKey(b.build().toByteString());
        Value existing = txn.get(btree, key.asReadOnlyByteBuffer());
        if (existing == null) {
            return null;
        }
        return Keyspace.user(Ints.checkedCast(existing.asLong()));
    }

    public Keyspace ensureKeyspace(WriteTransaction txn, KeyspaceName keyspaceName) {
        // TODO: Cache

        long nextId = -1;

        ByteString key = KEYSPACE_NAME_TO_ID.mapToKey(keyspaceName.toByteString());
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
            txn.doAction(btree, new SimpleSetOperation(KEYSPACE_ID_TO_NAME.mapToKey(ByteStrings.encode(id)),
                    stringValue));

            return Keyspace.user(Ints.checkedCast(id));
        } else {
            return Keyspace.user(Ints.checkedCast(existing.asLong()));
        }
    }
}
