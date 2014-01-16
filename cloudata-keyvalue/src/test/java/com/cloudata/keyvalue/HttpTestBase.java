package com.cloudata.keyvalue;

import static com.cloudata.TestUtils.buildBytes;

import java.io.IOException;

import org.junit.Assert;

import com.cloudata.keyvalue.KeyValueClient.KeyValueEntry;
import com.google.protobuf.ByteString;

public abstract class HttpTestBase extends IntegrationTestBase {

    protected void checkWrite(KeyValueClient client, long storeId, int n) throws Exception {
        for (int i = 1; i <= n; i++) {
            byte[] key = buildKey(i);

            byte[] data = buildBytes(i);
            client.put(storeId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }
    }

    protected void checkRead(KeyValueClient client, long storeId, int n) throws IOException {
        for (int i = 1; i <= n; i++) {
            byte[] key = buildKey(i);

            KeyValueEntry entry = client.read(storeId, ByteString.copyFrom(key));
            Assert.assertTrue("Entry not found: " + i, entry != null);
            byte[] data = entry.getValue().toByteArray();
            byte[] expected = buildBytes(i);
            Assert.assertArrayEquals(expected, data);
        }
    }

    protected byte[] buildKey(int i) {
        byte[] key = String.format("%04x", i).getBytes();
        return key;
    }

}
