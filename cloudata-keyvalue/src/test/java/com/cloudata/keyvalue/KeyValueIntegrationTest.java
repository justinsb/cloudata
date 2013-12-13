package com.cloudata.keyvalue;

import org.junit.Assert;
import org.junit.Test;

import com.cloudata.keyvalue.KeyValueClient.KeyValueEntry;
import com.google.protobuf.ByteString;

public class KeyValueIntegrationTest extends IntegrationTestBase {

    @Test
    public void testSetAndGet() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        KeyValueClient client = new KeyValueClient(url);

        int n = 20;

        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = buildValue(i);
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }

        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueEntry entry = client.read(logId, ByteString.copyFrom(key));
            byte[] data = entry.getValue().toByteArray();
            byte[] expected = buildValue(i);
            Assert.assertArrayEquals(expected, data);
        }
    }

    @Test
    public void testIncrement() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        KeyValueClient client = new KeyValueClient(url);

        int n = 20;

        for (int i = 1; i < n; i++) {
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

        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = buildValue(i);
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));

            KeyValueEntry entry = client.read(logId, ByteString.copyFrom(key));
            Assert.assertNotNull(entry);

            byte[] actual = entry.getValue().toByteArray();
            byte[] expected = buildValue(i);
            Assert.assertArrayEquals(expected, actual);
        }
    }

    @Test
    public void testPageSplit() throws Exception {
        // We set values that are too big for one page (32KB currently),
        // but aren't individually bigger than a page
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        KeyValueClient client = new KeyValueClient(url);

        int n = 30;

        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = buildValue(i * 1000);
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }

        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueEntry entry = client.read(logId, ByteString.copyFrom(key));
            byte[] data = entry.getValue().toByteArray();
            byte[] expected = buildValue(i * 1000);
            Assert.assertArrayEquals(expected, data);
        }
    }

    @Test
    public void testHugeValues() throws Exception {
        // We set values that are too big for a short
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        KeyValueClient client = new KeyValueClient(url);

        int n = 30;

        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = buildValue(i * 10000);
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }

        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueEntry entry = client.read(logId, ByteString.copyFrom(key));
            byte[] data = entry.getValue().toByteArray();
            byte[] expected = buildValue(i * 10000);
            Assert.assertArrayEquals(expected, data);
        }
    }

    @Test
    public void testSpaceReclamation() throws Exception {
        // We set values that are too big for a short
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        KeyValueClient client = new KeyValueClient(url);

        byte[] key = "A".getBytes();
        byte[] data = buildValue(30000);

        for (int i = 1; i < 10000; i++) {
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }
    }

    @Test
    public void testReplaceValue() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        KeyValueClient client = new KeyValueClient(url);

        int n = 20;

        // Set i = i
        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = buildValue(i);
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }

        // Set i = i * 2
        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = buildValue(i * 2);
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }

        // Check i = i * 2
        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueEntry entry = client.read(logId, ByteString.copyFrom(key));
            byte[] data = entry.getValue().toByteArray();
            byte[] expected = buildValue(i * 2);
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
        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = buildValue(i);
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }

        // Delete even keys
        for (int i = 1; i < n; i++) {
            if (i % 2 == 0) {
                byte[] key = Integer.toString(i).getBytes();
                client.delete(logId, ByteString.copyFrom(key));
            }
        }

        // Check
        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueEntry entry = client.read(logId, ByteString.copyFrom(key));
            if (i % 2 == 0) {
                Assert.assertNull(entry);
            } else {
                Assert.assertNotNull(entry);
                byte[] data = entry.getValue().toByteArray();
                byte[] expected = buildValue(i);
                Assert.assertArrayEquals(expected, data);
            }
        }
    }

}
