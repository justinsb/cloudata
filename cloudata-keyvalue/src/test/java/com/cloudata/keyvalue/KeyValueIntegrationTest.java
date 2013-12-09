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

        // TODO: Remove the need for a sleep
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
    public void simpleTest() throws Exception {
        String url = SERVERS[0].getHttpUrl();

        long logId = 2;

        KeyValueClient client = new KeyValueClient(url);

        int n = 20;

        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            byte[] data = new byte[i];
            for (int j = 0; j < i; j++) {
                data[j] = (byte) (j % 0xff);
            }
            client.put(logId, ByteString.copyFrom(key), ByteString.copyFrom(data));
        }

        // TODO: Remove the need for a sleep... wait for commit
        Thread.sleep(1000);

        for (int i = 1; i < n; i++) {
            byte[] key = Integer.toString(i).getBytes();
            KeyValueEntry entry = client.read(logId, ByteString.copyFrom(key));
            byte[] data = entry.getValue().toByteArray();
            Assert.assertEquals(i, data.length);

            for (int j = 0; j < i; j++) {
                Assert.assertEquals((byte) (j % 0xff), data[j]);
            }
        }

    }
}
