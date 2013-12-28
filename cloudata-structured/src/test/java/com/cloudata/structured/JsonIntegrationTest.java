package com.cloudata.structured;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cloudata.structured.StructuredClient.KeyValueJsonEntry;
import com.cloudata.structured.StructuredClient.KeyValueJsonRecordset;
import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;

public class JsonIntegrationTest extends IntegrationTestBase {

    protected static JsonObject buildJson(int n) {
        JsonObject o = new JsonObject();
        for (int i = 0; i < n; i++) {
            o.addProperty("key" + i, "value" + i);
        }
        return o;
    }

    @Test
    public void testSetAndGet() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();
        ByteString keyspace = ByteString.copyFromUtf8("space1");

        StructuredClient client = new StructuredClient(url);

        int n = 20;

        for (int i = 1; i <= n; i++) {
            ByteString key = ByteString.copyFromUtf8("" + i);
            JsonObject data = buildJson(i);
            client.put(logId, keyspace, key, data);
        }

        for (int i = 1; i <= n; i++) {
            ByteString key = ByteString.copyFromUtf8("" + i);
            KeyValueJsonEntry entry = client.readJson(logId, keyspace, key);

            Assert.assertNotNull(entry);

            JsonElement data = entry.getValue();
            JsonObject expected = buildJson(i);
            Assert.assertEquals(expected, data);
        }

        List<String> keys = Lists.newArrayList();
        for (ByteString key : client.getKeys(logId, keyspace)) {
            keys.add(key.toStringUtf8());
        }

        List<String> expected = Lists.newArrayList();
        for (int i = 0; i < n; i++) {
            expected.add("key" + i);
        }

        Collections.sort(keys);
        Collections.sort(expected);

        Assert.assertEquals(expected, keys);
    }

    @Test
    public void testSetAndQuery() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long storeId = newLogId();
        ByteString keyspace = ByteString.copyFromUtf8("space1");

        StructuredClient client = new StructuredClient(url);

        int n = 20;

        for (int i = 1; i <= n; i++) {
            ByteString key = ByteString.copyFromUtf8(String.format("%04x", i));
            JsonObject data = buildJson(i);
            client.put(storeId, keyspace, key, data);
        }

        try (KeyValueJsonRecordset rs = client.queryJson(storeId, keyspace)) {
            ArrayList<KeyValueJsonEntry> entries = Lists.newArrayList(rs);

            Assert.assertEquals(n, entries.size());

            for (int i = 1; i <= n; i++) {
                ByteString expectedKey = ByteString.copyFromUtf8(String.format("%04x", i));
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
        ByteString keyspace = ByteString.copyFromUtf8("space1");

        StructuredClient client = new StructuredClient(url);

        int n = 20;

        for (int i = 1; i <= n; i++) {
            ByteString key = ByteString.copyFromUtf8(String.format("%04x", i));
            JsonObject data = buildJson(i);
            client.put(logId, keyspace, key, data);

            KeyValueJsonEntry entry = client.readJson(logId, keyspace, key);
            Assert.assertNotNull(entry);

            JsonElement actual = entry.getValue();
            JsonObject expected = buildJson(i);
            Assert.assertEquals(expected, actual);
        }
    }

    // @Test
    // public void testDynamicKeys() throws Exception {
    // // We set values that are too big for a short
    // String url = SERVERS[0].getHttpUrl();
    //
    // long logId = newLogId();
    //
    // StructuredClient client = new StructuredClient(url);
    //
    // int n = 30;
    //
    // for (int i = 1; i <= n; i++) {
    // byte[] key = Integer.toString(i).getBytes();
    // JsonObject data = buildJson(i * 10000);
    // client.put(logId, ByteString.copyFrom(key), data);
    // }
    //
    // for (int i = 1; i <= n; i++) {
    // byte[] key = Integer.toString(i).getBytes();
    // KeyValueJsonEntry entry = client.readJson(logId, ByteString.copyFrom(key));
    // JsonElement data = entry.getValue();
    // JsonObject expected = buildJson(i * 10000);
    // Assert.assertEquals(expected, data);
    // }
    // }

    @Test
    public void testReplaceValue() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();
        ByteString keyspace = ByteString.copyFromUtf8("space1");

        StructuredClient client = new StructuredClient(url);

        int n = 20;

        // Set i = i
        for (int i = 1; i <= n; i++) {
            ByteString key = ByteString.copyFromUtf8(String.format("%04x", i));

            JsonObject data = buildJson(i);
            client.put(logId, keyspace, key, data);
        }

        // Set i = i * 2
        for (int i = 1; i <= n; i++) {
            ByteString key = ByteString.copyFromUtf8(String.format("%04x", i));
            JsonObject data = buildJson(i * 2);
            client.put(logId, keyspace, key, data);
        }

        // Check i = i * 2
        for (int i = 1; i <= n; i++) {
            ByteString key = ByteString.copyFromUtf8(String.format("%04x", i));

            KeyValueJsonEntry entry = client.readJson(logId, keyspace, key);
            JsonElement data = entry.getValue();
            JsonObject expected = buildJson(i * 2);
            Assert.assertEquals(expected, data);
        }
    }

    @Test
    public void testDelete() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = newLogId();
        ByteString keyspace = ByteString.copyFromUtf8("space1");

        StructuredClient client = new StructuredClient(url);

        int n = 20;

        // Set i = i
        for (int i = 1; i <= n; i++) {
            ByteString key = ByteString.copyFromUtf8(String.format("%04x", i));
            JsonObject data = buildJson(i);
            client.put(logId, keyspace, key, data);
        }

        // Delete even keys
        for (int i = 1; i <= n; i++) {
            if (i % 2 == 0) {
                ByteString key = ByteString.copyFromUtf8(String.format("%04x", i));
                client.delete(logId, keyspace, key);
            }
        }

        // Check
        for (int i = 1; i <= n; i++) {
            ByteString key = ByteString.copyFromUtf8(String.format("%04x", i));

            KeyValueJsonEntry entry = client.readJson(logId, keyspace, key);
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
