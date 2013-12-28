package com.cloudata.structured;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import com.cloudata.structured.StructuredClient.RowEntry;
import com.cloudata.structured.StructuredClient.RowRecordset;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;

public class SqlIntegrationTest extends IntegrationTestBase {

    protected static JsonObject buildSqlJson(int n) {
        JsonObject o = new JsonObject();
        for (int i = 1; i <= n; i++) {
            o.addProperty("column" + i, "value" + i);
        }
        return o;
    }

    @Test
    public void testSetAndQuery() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long storeId = newLogId();
        ByteString keyspace = ByteString.copyFromUtf8("space1");

        StructuredClient client = new StructuredClient(url);

        int n = 20;

        for (int i = 1; i <= n; i++) {
            byte[] key = String.format("%04x", i).getBytes();
            JsonObject data = buildSqlJson(i);
            client.put(storeId, keyspace, ByteString.copyFrom(key), data);
        }

        int reps = 1;
        for (int rep = 1; rep <= reps; rep++) {
            String sql = "SELECT column1, column2, column3 FROM space1";
            try (RowRecordset rs = client.queryJson(storeId, sql)) {
                ArrayList<RowEntry> entries = Lists.newArrayList(rs);

                Assert.assertEquals(n, entries.size());

                for (int i = 1; i <= n; i++) {
                    // ByteString expectedKey = Keyspace.ZERO.mapToKey(String.format("%04x", i).getBytes());
                    // JsonObject expectedValue = buildJson(i);
                    RowEntry entry = entries.get(i - 1);

                    // System.out.println(entry);
                    Assert.assertEquals(3, entry.size());
                    Assert.assertEquals("value1", entry.get(0).getAsString());
                    if (i >= 2) {
                        Assert.assertEquals("value2", entry.get(1).getAsString());
                    } else {
                        Assert.assertTrue(entry.get(1).isNull());
                    }
                    if (i >= 3) {
                        Assert.assertEquals("value3", entry.get(2).getAsString());
                    } else {
                        Assert.assertTrue(entry.get(2).isNull());
                    }
                }
            }
        }
    }

}
