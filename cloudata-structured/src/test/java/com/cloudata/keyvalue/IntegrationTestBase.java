package com.cloudata.keyvalue;

import java.io.File;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.robotninjas.barge.Replica;

import com.cloudata.structured.StructuredServer;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class IntegrationTestBase {
    protected static StructuredServer[] SERVERS;
    static File TEMPDIR;

    @BeforeClass
    public static void startServers() throws Exception {
        TEMPDIR = Files.createTempDir();

        SERVERS = new StructuredServer[3];

        for (int i = 0; i < SERVERS.length; i++) {
            Replica local = Replica.fromString("localhost:" + (10000 + i));
            List<Replica> members = Lists.newArrayList();
            for (int j = 0; j < SERVERS.length; j++) {
                members.add(Replica.fromString("localhost:" + (10000 + j)));
            }
            members.remove(local);

            int httpPort = 9990 + i;

            File baseDir = new File(TEMPDIR, "" + i);
            SERVERS[i] = new StructuredServer(baseDir, local, members, httpPort);

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
