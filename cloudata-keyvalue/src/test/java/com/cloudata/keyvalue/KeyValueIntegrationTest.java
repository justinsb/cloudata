package com.cloudata.keyvalue;

import java.io.File;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.robotninjas.barge.Replica;

import com.cloudata.keyvalue.KeyValueClient.KeyValueEntry;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.protobuf.ByteString;

public class KeyValueIntegrationTest {

    static KeyValueServer[] SERVERS;
    static File TEMPDIR;

    @BeforeClass
    public static void startServers() throws Exception {
        TEMPDIR = Files.createTempDir();

        SERVERS = new KeyValueServer[3];

        for (int i = 0; i < SERVERS.length; i++) {
            Replica local = Replica.fromString("localhost:" + (10000 + i));
            List<Replica> members = Lists.newArrayList();
            for (int j = 0; j < SERVERS.length; j++) {
                members.add(Replica.fromString("localhost:" + (10000 + j)));
            }
            members.remove(local);

            int httpPort = 9990 + i;

            File baseDir = new File(TEMPDIR, "" + i);
            SERVERS[i] = new KeyValueServer(baseDir, local, members, httpPort);

            SERVERS[i].start();
        }

        // TODO: Remove the need for a sleep here
        Thread.sleep(1000);
    }

    @AfterClass
    public static void stopServers() {
        for (int i = 0; i < SERVERS.length; i++) {
            SERVERS[i].stop();
        }

        rmdir(TEMPDIR);
    }

    private static void rmdir(File dir) {
        for (File f : dir.listFiles()) {
            if (f.isFile()) {
                f.delete();
            } else {
                rmdir(f);
            }
        }

        dir.delete();
    }

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

    static long nextLogId = 1;

    private long newLogId() {
        return nextLogId++;
    }

    private byte[] buildValue(int i) {
        byte[] data = new byte[i];
        for (int j = 0; j < i; j++) {
            data[j] = (byte) (j % 0xff);
        }
        return data;
    }
}
