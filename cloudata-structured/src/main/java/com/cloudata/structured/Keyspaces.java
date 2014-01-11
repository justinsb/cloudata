package com.cloudata.structured;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.WellKnownKeyspaces;
import com.cloudata.btree.Btree;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.Transaction;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.btree.operation.SimpleSetOperation;
import com.cloudata.clients.structured.AlreadyExistsException;
import com.cloudata.structured.StructuredProtocol.KeyspaceData;
import com.cloudata.structured.StructuredProtocol.KeyspaceData.Builder;
import com.cloudata.structured.StructuredProtocol.KeyspaceName;
import com.cloudata.structured.StructuredProtocol.KeyspaceType;
import com.cloudata.util.Codecs;
import com.cloudata.values.QualifiedKeyValue;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class Keyspaces {
    private static final Logger log = LoggerFactory.getLogger(Keyspaces.class);

    final Btree btree;

    static final Keyspace KEYSPACE_DEFINITIONS = Keyspace.system(SystemKeyspaces.KEYSPACE_DEFINITIONS);
    static final Keyspace KEYSPACE_DEFINITIONS_IX_NAME = Keyspace.system(SystemKeyspaces.KEYSPACE_DEFINITIONS_IX_NAME);

    public Keyspaces(Btree btree) {
        this.btree = btree;
    }

    public static KeyspaceName buildUserKeyspaceName(String name) {
        KeyspaceName.Builder b = KeyspaceName.newBuilder();
        b.setType(KeyspaceType.USER_DATA);
        b.setName(ByteString.copyFromUtf8(name));
        return b.build();
    }

    public Integer findKeyspaceId(KeyspaceName keyspaceName) {
        try (Transaction txn = btree.beginReadOnly()) {
            return findKeyspaceId(txn, keyspaceName);
        }
    }

    public Integer findKeyspaceId(Transaction txn, KeyspaceName keyspaceName) {
        ByteString key = KEYSPACE_DEFINITIONS_IX_NAME.mapToKey(keyspaceName.toByteString());
        Value existing = txn.get(btree, key.asReadOnlyByteBuffer());
        if (existing == null) {
            return null;
        }

        QualifiedKeyValue qualifiedKeyValue = (QualifiedKeyValue) existing;
        ByteString rawId = qualifiedKeyValue.getUnqualified();
        return Codecs.decodeInt32(rawId);
    }

    public KeyspaceData readKeyspaceData(Transaction txn, int id) throws IOException {
        ByteString key = KEYSPACE_DEFINITIONS.mapToKey(Codecs.encodeInt32(id));
        Value existing = txn.get(btree, key.asReadOnlyByteBuffer());
        if (existing == null) {
            return null;
        }

        Builder b = KeyspaceData.newBuilder();
        try {
            existing.asProtobuf(b);
        } catch (IOException e) {
            throw new IOException("Error parsing value", e);
        }
        return b.build();
    }

    // public Keyspace ensureKeyspace(WriteTransaction txn, KeyspaceName keyspaceName) {
    // // TODO: Cache
    //
    // long nextId = -1;
    //
    // ByteString key = KEYSPACE_NAME_TO_ID.mapToKey(keyspaceName.toByteString());
    // Value existing = txn.get(btree, key.asReadOnlyByteBuffer());
    // if (existing == null) {
    // if (nextId < 0) {
    // log.warn("Find max nextId logic is stupid");
    // FindMaxListener listener = new FindMaxListener(KEYSPACE_ID_TO_NAME);
    // txn.walk(btree, KEYSPACE_ID_TO_NAME.mapToKey(ByteString.EMPTY).asReadOnlyByteBuffer(), listener);
    // ByteBuffer lastKey = listener.getLastKey();
    // if (lastKey == null || !KEYSPACE_ID_TO_NAME.contains(lastKey)) {
    // nextId = 1;
    // } else {
    // ByteBuffer suffix = KEYSPACE_ID_TO_NAME.getSuffix(lastKey);
    // long lastId = suffix.getLong();
    // nextId = lastId + 1;
    // }
    // }
    //
    // assert nextId > 0;
    //
    // long id = nextId++;
    //
    // Value idValue = Value.fromLong(id);
    // txn.doAction(btree, new SimpleSetOperation(key, idValue));
    //
    // KeyspaceData.Builder b = KeyspaceData.newBuilder();
    // b.setId(id);
    // b.setName(keyspaceName);
    // Value stringValue = Value.fromRawBytes(b.build().toByteString());
    // txn.doAction(btree, new SimpleSetOperation(KEYSPACE_ID_TO_NAME.mapToKey(ByteStrings.encode(id)),
    // stringValue));
    //
    // return Keyspace.user(Ints.checkedCast(id));
    // } else {
    // return Keyspace.user(Ints.checkedCast(existing.asLong()));
    // }
    // }

    public KeyspaceData createKeyspace(WriteTransaction txn, KeyspaceData keyspaceData) throws AlreadyExistsException {
        KeyspaceName keyspaceName = keyspaceData.getName();

        Preconditions.checkState(keyspaceName.getType() == KeyspaceType.USER_DATA);
        Preconditions.checkArgument(keyspaceName.hasName());

        ByteString indexQualifiedKey = KEYSPACE_DEFINITIONS_IX_NAME.mapToKey(keyspaceName.toByteString());

        {
            Value existing = txn.get(btree, indexQualifiedKey.asReadOnlyByteBuffer());
            if (existing == null) {
                throw new AlreadyExistsException();
            }
        }

        // TODO: Cache
        int nextId = -1;

        if (nextId < 0) {
            ByteBuffer lastKey = Btrees.findLast(btree, txn, KEYSPACE_DEFINITIONS);
            if (lastKey == null) {
                nextId = 1;
            } else {
                assert KEYSPACE_DEFINITIONS.contains(lastKey);

                ByteBuffer suffix = KEYSPACE_DEFINITIONS.getSuffix(lastKey);
                int lastId = suffix.getInt();
                nextId = lastId + 1;
            }
        }

        assert nextId > 0;

        int id = nextId++;

        if (id >= WellKnownKeyspaces.SYSTEM_START) {
            throw new IllegalStateException();
        }

        ByteString primaryKey = Codecs.encodeInt32(id);
        ByteString qualifiedPrimaryKey = KEYSPACE_DEFINITIONS.mapToKey(primaryKey);

        // Update data with id
        {
            KeyspaceData.Builder b = KeyspaceData.newBuilder(keyspaceData);
            b.setId(id);
            keyspaceData = b.build();
        }

        // Insert into definition keyspace
        {
            Value v = Value.fromProtobuf(keyspaceData);
            txn.doAction(btree, new SimpleSetOperation(qualifiedPrimaryKey, v));
        }

        // Insert into name index
        {
            // Note: we store the _qualified_ primary key.
            // It doesn't cost us much, and may be useful:
            // 1) To know that it's a relationship
            // 2) To support 'inheritence' or 'flexible' relationships
            Value idValue = Value.fromQualifiedKey(qualifiedPrimaryKey);
            txn.doAction(btree, new SimpleSetOperation(indexQualifiedKey, idValue));
        }

        return keyspaceData;
    }
}
