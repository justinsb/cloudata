package com.cloudata.keyvalue;

import static com.cloudata.TestUtils.buildBytes;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import com.cloudata.btree.Keyspace;
import com.cloudata.keyvalue.KeyValueClient.KeyValueEntry;
import com.cloudata.keyvalue.KeyValueClient.KeyValueRecordset;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

public class KeyValueIntegrationTest extends IntegrationTestBase {

    @Test
    public void testSetAndGet() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        KeyValueClient client = new KeyValueClient(url);

        int n = 20;

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = buildBytes(i);
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueEntry entry = client.read(logId, ByteString.copyFrom(key));
            byte[] data = entry.getValue().toByteArray();
            byte[] expected = buildBytes(i);
            Assert.assertArrayEquals(expected, data);
        }
    }

    @Test
    public void testSetAndQuery() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long storeId = newLogId();

        KeyValueClient client = new KeyValueClient(url);

        int n = 20;

        for (int i = 1; i <= n; i++) {
            byte[] key = String.format("%04x", i).getBytes();
            byte[] data = buildBytes(i);
            client.put(storeId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }

        try (KeyValueRecordset rs = client.query(storeId)) {
            ArrayList<KeyValueEntry> entries = Lists.newArrayList(rs);

            Assert.assertEquals(n, entries.size());

            for (int i = 1; i <= n; i++) {
                ByteString expectedKey = Keyspace.ZERO.mapToKey(String.format("%04x", i).getBytes());
                byte[] expectedValue = buildBytes(i);
                KeyValueEntry entry = entries.get(i - 1);
                Assert.assertEquals(expectedKey, entry.getKey());
                Assert.assertArrayEquals(expectedValue, entry.getValue().toByteArray());
            }
        }
    }

    @Test
    public void testIncrement() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        KeyValueClient client = new KeyValueClient(url);

        int n = 20;

        for (int i = 1; i <= n; i++) {
            byte[] key = "A".getBytes();
            KeyValueEntry entry = client.increment(logId, ByteString.copyFrom(key));
            Assert.assertNotNull(entry);
            byte[] actual = entry.getValue().toByteArray();
            byte[] expected = Integer.toString(i).getBytes();
            Assert.assertArrayEquals(expected, actual);
        }
    }

    @Test
    public void testReadYourWrites() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        KeyValueClient client = new KeyValueClient(url);

        int n = 20;

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = buildBytes(i);
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));

            KeyValueEntry entry = client.read(logId, ByteString.copyFrom(key));
            Assert.assertNotNull(entry);

            byte[] actual = entry.getValue().toByteArray();
            byte[] expected = buildBytes(i);
            Assert.assertArrayEquals(expected, actual);
        }
    }

    @Test
    public void testHugeValues() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        KeyValueClient client = new KeyValueClient(url);

        int n = 30;

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = buildBytes(i * 10000);
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueEntry entry = client.read(logId, ByteString.copyFrom(key));
            byte[] data = entry.getValue().toByteArray();
            byte[] expected = buildBytes(i * 10000);
            Assert.assertArrayEquals(expected, data);
        }
    }

    @Test
    public void testReplaceValue() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        KeyValueClient client = new KeyValueClient(url);

        int n = 20;

        // Set i = i
        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = buildBytes(i);
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }

        // Set i = i * 2
        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = buildBytes(i * 2);
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }

        // Check i = i * 2
        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueEntry entry = client.read(logId, ByteString.copyFrom(key));
            byte[] data = entry.getValue().toByteArray();
            byte[] expected = buildBytes(i * 2);
            Assert.assertArrayEquals(expected, data);
        }
    }

    @Test
    public void testDelete() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        KeyValueClient client = new KeyValueClient(url);

        int n = 20;

        // Set i = i
        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = buildBytes(i);
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }

        // Delete even keys
        for (int i = 1; i <= n; i++) {
            if (i % 2 == 0) {
                byte[] key = Integer.toString(i).getBytes();
                client.delete(logId, ByteString.copyFrom(key));
            }
        }

        // Check
        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueEntry entry = client.read(logId, ByteString.copyFrom(key));
            if (i % 2 == 0) {
                Assert.assertNull(entry);
            } else {
                Assert.assertNotNull(entry);
                byte[] data = entry.getValue().toByteArray();
                byte[] expected = buildBytes(i);
                Assert.assertArrayEquals(expected, data);
            }
        }
    }

}
