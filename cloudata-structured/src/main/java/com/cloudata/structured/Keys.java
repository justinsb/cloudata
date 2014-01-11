package com.cloudata.structured;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.Btree;
import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.BtreeQuery.KeyValueResultset;
import com.cloudata.btree.EntryListener;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.btree.operation.SimpleSetOperation;
import com.cloudata.util.Codecs;
import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class Keys {
    private static final Logger log = LoggerFactory.getLogger(Keys.class);

    static final Keyspace KEYSPACE_KEYS = Keyspace.system(SystemKeyspaces.KEYSPACE_KEYS);
    static final Keyspace KEYSPACE_KEYS_IX_KEYSPACE_NAME = Keyspace
            .system(SystemKeyspaces.KEYSPACE_KEYS_IX_KEYSPACE_NAME);

    final Btree btree;

    public Keys(Btree btree) {
        this.btree = btree;
    }

    public void listKeys(final Keyspace forKeyspace, final Listener<ByteBuffer> listener) {
        boolean stripKeyspace = false;
        ByteString prefix = forKeyspace.getKeyspacePrefix();

        BtreeQuery query = new BtreeQuery(btree, KEYSPACE_KEYS_IX_KEYSPACE_NAME, stripKeyspace, prefix);

        final int nameOffset = KEYSPACE_KEYS_IX_KEYSPACE_NAME.getKeyspacePrefix().size() + prefix.size();

        try (KeyValueResultset cursor = query.buildCursor()) {
            EntryListener entryListener = new EntryListener() {
                @Override
                public boolean found(ByteBuffer key, Value value) {
                    ByteBuffer name = key.duplicate();
                    name.position(name.position() + nameOffset);
                    return listener.next(name);
                }
            };
            cursor.walk(entryListener);
        }
    }

    public void ensureKeys(WriteTransaction txn, Keyspace forKeyspace, Iterable<ByteString> keys) {
        // TODO: Cache

        long nextId = -1;

        for (ByteString key : keys) {
            ByteString qualifiedIndexKey = KEYSPACE_KEYS_IX_KEYSPACE_NAME.mapToKey(forKeyspace.mapToKey(key));
            Value existing = txn.get(btree, qualifiedIndexKey.asReadOnlyByteBuffer());
            if (existing == null) {
                if (nextId < 0) {
                    ByteBuffer lastKey = Btrees.findLast(btree, txn, KEYSPACE_KEYS);

                    if (lastKey != null) {
                        nextId = 1;
                    } else {
                        ByteBuffer suffix = KEYSPACE_KEYS.getSuffix(lastKey);
                        long lastId = Codecs.decodeInt64(suffix);
                        nextId = lastId + 1;
                    }
                }

                assert nextId > 0;

                long id = nextId++;

                Value idValue = Value.fromLong(id);
                txn.doAction(btree, new SimpleSetOperation(qualifiedIndexKey, idValue));

                ByteString qualifiedPrimaryKey = KEYSPACE_KEYS.mapToKey(Codecs.encodeInt64(id));
                Value stringValue = Value.fromRawBytes(key);
                txn.doAction(btree, new SimpleSetOperation(qualifiedPrimaryKey, stringValue));
            }
        }
    }

}
