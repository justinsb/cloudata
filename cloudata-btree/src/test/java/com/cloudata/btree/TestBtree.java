package com.cloudata.btree;

import static com.cloudata.TestUtils.buildBytes;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.cloudata.TestUtils;
import com.cloudata.btree.operation.SimpleSetOperation;
import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class TestBtree {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testSpaceReclamation() throws Exception {
        byte[] keyBytes = randomBytes(16);
        Btree btree = buildBtree(keyBytes);

        byte[] key = "A".getBytes();
        byte[] bytes = TestUtils.buildBytes(30000);

        for (int i = 1; i <= 10000; i++) {
            put(btree, key, bytes);
        }

        System.out.println(btree.getDb().getPageStore().debugDump());
        Assert.assertTrue(btree.getDb().getPageStore().debugIsIdle().or(true));
    }

    static final Random random = new Random();

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    private void put(Btree btree, byte[] key, byte[] valueBytes) throws IOException {
        try (WriteTransaction txn = btree.beginReadWrite()) {
            Value value = Value.fromRawBytes(valueBytes);
            SimpleSetOperation operation = new SimpleSetOperation(ByteString.copyFrom(key), value);
            txn.doAction(btree, operation);
            txn.commit();
        }
    }

    private Btree buildBtree(byte[] keyBytes) throws IOException {
        File file = new File(folder.getRoot(), UUID.randomUUID().toString());
        return buildBtree(keyBytes, file);
    }

    private Btree buildBtree(byte[] keyBytes, File file) throws IOException {
        boolean uniqueKeys = true;
        Database db = Database.build(file, keyBytes, null, null);
        Btree btree = new Btree(db, uniqueKeys);
        return btree;
    }

    @Test
    public void testPageSplit() throws Exception {
        byte[] keyBytes = randomBytes(16);
        Btree btree = buildBtree(keyBytes);

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

        System.out.println(btree.getDb().getPageStore().debugDump());
        Assert.assertTrue(btree.getDb().getPageStore().debugIsIdle().or(true));
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
        byte[] keyBytes = randomBytes(16);
        Btree btree = buildBtree(keyBytes);

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

        System.out.println(btree.getDb().getPageStore().debugDump());
        Assert.assertTrue(btree.getDb().getPageStore().debugIsIdle().or(true));
    }

    @Test(expected = IOException.class)
    public void testWrongKey() throws Exception {
        byte[] keyBytes = randomBytes(16);
        File file = new File(folder.getRoot(), UUID.randomUUID().toString());

        {
            Database db = Database.build(file, keyBytes, null, null);
            boolean uniqueKeys = true;
            Btree btree = new Btree(db, uniqueKeys);

            simplePut(btree, 100);

            simpleRead(btree, 100);

            Assert.assertTrue(btree.getDb().getPageStore().debugIsIdle().or(true));
            db.close();
        }

        keyBytes = randomBytes(16);
        {
            Database db = Database.build(file, keyBytes, null, null);
            boolean uniqueKeys = true;

            // Should throw
            Btree btree = new Btree(db, uniqueKeys);

            simpleRead(btree, 100);

            Assert.assertTrue(btree.getDb().getPageStore().debugIsIdle().or(true));
            db.close();
        }
    }

    @Test
    public void testSimple() throws Exception {
        byte[] keyBytes = randomBytes(16);
        File file = new File(folder.getRoot(), UUID.randomUUID().toString());

        Database db = Database.build(file, keyBytes, null, null);
        boolean uniqueKeys = true;
        Btree btree = new Btree(db, uniqueKeys);

        simplePut(btree, 100);

        simpleRead(btree, 100);

        Assert.assertTrue(btree.getDb().getPageStore().debugIsIdle().or(true));
        db.close();
    }

    @Test
    public void testOpenAndClose() throws Exception {
        byte[] keyBytes = randomBytes(16);
        File file = new File(folder.getRoot(), UUID.randomUUID().toString());

        for (int i = 0; i < 3; i++) {
            Database db = Database.build(file, keyBytes, null, null);
            boolean uniqueKeys = true;
            Btree btree = new Btree(db, uniqueKeys);

            if (i == 0) {
                simplePut(btree, 100);
            }

            simpleRead(btree, 100);

            System.out.println(btree.getDb().getPageStore().debugDump());
            System.out.flush();

            Assert.assertTrue(btree.getDb().getPageStore().debugIsIdle().or(true));
            db.close();
        }

    }

    private void simplePut(Btree btree, int n) throws IOException {
        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] value = buildBytes(i);
            put(btree, key, value);
        }
    }

    private void simpleRead(Btree btree, int n) {
        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] actual = get(btree, key);
            byte[] expected = buildBytes(i);
            Assert.assertArrayEquals(expected, actual);
        }
    }
}
