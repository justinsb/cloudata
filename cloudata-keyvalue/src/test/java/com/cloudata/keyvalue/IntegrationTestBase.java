package com.cloudata.keyvalue;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.robotninjas.barge.Replica;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class IntegrationTestBase {
    protected static KeyValueServer[] SERVERS;
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
            SocketAddress redisAddress = new InetSocketAddress(6379 + i + 1);
            SERVERS[i] = new KeyValueServer(baseDir, local, members, httpPort, redisAddress);

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

    static long nextLogId = 1;

    protected long newLogId() {
        return nextLogId++;
    }

    protected static byte[] buildBytes(int length) {
        byte[] data = new byte[length];
        for (int j = 0; j < length; j++) {
            data[j] = (byte) (j % 0xff);
        }
        return data;
    }

}
