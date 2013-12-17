package com.cloudata.keyvalue;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import com.cloudata.btree.Keyspace;
import com.cloudata.structured.StructuredClient;
import com.cloudata.structured.StructuredClient.KeyValueJsonEntry;
import com.cloudata.structured.StructuredClient.KeyValueJsonRecordset;
import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;

public class JsonIntegrationTest extends IntegrationTestBase {

    protected static JsonObject buildJson(int n) {
        JsonObject o = new JsonObject();
        o.addProperty("key" + n, "value" + n);
        return o;
    }

    @Test
    public void testSetAndGet() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        StructuredClient client = new StructuredClient(url);

        int n = 20;

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            JsonObject data = buildJson(i);
            client.put(logId, ByteString.copyFrom(key), data);
        }

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueJsonEntry entry = client.readJson(logId, ByteString.copyFrom(key));
            JsonElement data = entry.getValue();
            JsonObject expected = buildJson(i);
            Assert.assertEquals(expected, data);
        }
    }

    @Test
    public void testSetAndQuery() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long storeId = newLogId();

        StructuredClient client = new StructuredClient(url);

        int n = 20;

        for (int i = 1; i <= n; i++) {
            byte[] key = String.format("%04x", i).getBytes();
            JsonObject data = buildJson(i);
            client.put(storeId, ByteString.copyFrom(key), data);
        }

        try (KeyValueJsonRecordset rs = client.queryJson(storeId)) {
            ArrayList<KeyValueJsonEntry> entries = Lists.newArrayList(rs);

            Assert.assertEquals(n, entries.size());

            for (int i = 1; i <= n; i++) {
                ByteString expectedKey = Keyspace.ZERO.mapToKey(String.format("%04x", i).getBytes());
                JsonObject expectedValue = buildJson(i);
                KeyValueJsonEntry entry = entries.get(i - 1);
                Assert.assertEquals(expectedKey, entry.getKey());
                Assert.assertEquals(expectedValue, entry.getValue());
            }
        }
    }

    @Test
    public void testReadYourWrites() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        StructuredClient client = new StructuredClient(url);

        int n = 20;

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            JsonObject data = buildJson(i);
            client.put(logId, ByteString.copyFrom(key), data);

            KeyValueJsonEntry entry = client.readJson(logId, ByteString.copyFrom(key));
            Assert.assertNotNull(entry);

            JsonElement actual = entry.getValue();
            JsonObject expected = buildJson(i);
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testPageSplit() throws Exception {
        // We set values that are too big for one page (32KB currently),
        // but aren't individually bigger than a page
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        StructuredClient client = new StructuredClient(url);

        int n = 30;

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            JsonObject data = buildJson(i * 1000);
            client.put(logId, ByteString.copyFrom(key), data);
        }

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueJsonEntry entry = client.readJson(logId, ByteString.copyFrom(key));
            JsonElement data = entry.getValue();
            JsonObject expected = buildJson(i * 1000);
            Assert.assertEquals(expected, data);
        }
    }

    @Test
    public void testHugeValues() throws Exception {
        // We set values that are too big for a short
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        StructuredClient client = new StructuredClient(url);

        int n = 30;

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            JsonObject data = buildJson(i * 10000);
            client.put(logId, ByteString.copyFrom(key), data);
        }

        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueJsonEntry entry = client.readJson(logId, ByteString.copyFrom(key));
            JsonElement data = entry.getValue();
            JsonObject expected = buildJson(i * 10000);
            Assert.assertEquals(expected, data);
        }
    }

    @Test
    public void testSpaceReclamation() throws Exception {
        // We set values that are too big for a short
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        StructuredClient client = new StructuredClient(url);

        byte[] key = "A".getBytes();
        JsonObject data = buildJson(30000);

        for (int i = 1; i <= 10000; i++) {
            client.put(logId, ByteString.copyFrom(key), data);
        }
    }

    @Test
    public void testReplaceValue() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        StructuredClient client = new StructuredClient(url);

        int n = 20;

        // Set i = i
        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            JsonObject data = buildJson(i);
            client.put(logId, ByteString.copyFrom(key), data);
        }

        // Set i = i * 2
        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            JsonObject data = buildJson(i * 2);
            client.put(logId, ByteString.copyFrom(key), data);
        }

        // Check i = i * 2
        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueJsonEntry entry = client.readJson(logId, ByteString.copyFrom(key));
            JsonElement data = entry.getValue();
            JsonObject expected = buildJson(i * 2);
            Assert.assertEquals(expected, data);
        }
    }

    @Test
    public void testDelete() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();

        StructuredClient client = new StructuredClient(url);

        int n = 20;

        // Set i = i
        for (int i = 1; i <= n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            JsonObject data = buildJson(i);
            client.put(logId, ByteString.copyFrom(key), data);
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
            KeyValueJsonEntry entry = client.readJson(logId, ByteString.copyFrom(key));
            if (i % 2 == 0) {
                Assert.assertNull(entry);
            } else {
                Assert.assertNotNull(entry);
                JsonElement data = entry.getValue();
                JsonObject expected = buildJson(i);
                Assert.assertEquals(expected, data);
            }
        }
    }

}
