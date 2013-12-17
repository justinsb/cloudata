package com.cloudata.btree;

import static com.cloudata.TestUtils.buildBytes;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.cloudata.TestUtils;
import com.cloudata.btree.operation.SetOperation;
import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class TestBtree {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testSpaceReclamation() throws Exception {
        Btree btree = buildBtree();

        byte[] key = "A".getBytes();
        byte[] bytes = TestUtils.buildBytes(30000);

        for (int i = 1; i <= 10000; i++) {
            put(btree, key, bytes);
        }
    }

    private void put(Btree btree, byte[] key, byte[] valueBytes) {
        try (WriteTransaction txn = btree.beginReadWrite()) {
            Value value = Value.fromRawBytes(valueBytes);
            BtreeOperation<Void> operation = new SetOperation(value);
            txn.doAction(btree, ByteString.copyFrom(key).asReadOnlyByteBuffer(), operation);
            txn.commit();
        }
    }

    private Btree buildBtree() throws IOException {
        File data = new File(folder.getRoot(), UUID.randomUUID().toString());
        boolean uniqueKeys = true;
        PageStore pageStore = MmapPageStore.build(data, uniqueKeys);

        Btree btree = new Btree(pageStore, uniqueKeys);
        return btree;
    }

    @Test
    public void testPageSplit() throws Exception {
        Btree btree = buildBtree();

        int n = 30;

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] value = buildBytes(i * 1000);
            put(btree, key, value);
        }

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] actual = get(btree, key);
            byte[] expected = buildBytes(i * 1000);
            Assert.assertArrayEquals(expected, actual);
        }
    }

    private byte[] get(Btree btree, byte[] key) {
        try (ReadOnlyTransaction txn = btree.beginReadOnly()) {
            Value value = txn.get(btree, ByteBuffer.wrap(key));
            if (value == null) {
                return null;
            }
            // Inefficient: only for test code!
            return ByteString.copyFrom(value.asBytes()).toByteArray();
        }
    }

    @Test
    public void testHugeValues() throws Exception {
        Btree btree = buildBtree();

        int n = 30;

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] value = buildBytes(i * 10000);
            put(btree, key, value);
        }

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] actual = get(btree, key);
            byte[] expected = buildBytes(i * 10000);
            Assert.assertArrayEquals(expected, actual);
        }
    }
}
