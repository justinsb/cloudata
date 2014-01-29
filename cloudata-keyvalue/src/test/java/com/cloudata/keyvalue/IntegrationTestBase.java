package com.cloudata.keyvalue;

import java.io.File;
import java.util.List;

import org.junit.AfterClass;
import org.robotninjas.barge.Replica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.TestUtils;
import com.cloudata.cluster.GossipConfig;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.net.HostAndPort;

public class IntegrationTestBase {
    private static final Logger log = LoggerFactory.getLogger(IntegrationTestBase.class);

    protected static List<KeyValueServer> SERVERS;
    static File TEMPDIR;

    static {
        resetState();
    }

    public static void resetState() {
        SERVERS = Lists.newArrayList();
        if (TEMPDIR != null) {
            TestUtils.rmdir(TEMPDIR);
            TEMPDIR = null;
        }
        TEMPDIR = Files.createTempDir();
    }

    static void startCluster(int n, boolean bootstrap) throws Exception {
        if (bootstrap) {
            addServers(1, false);
            SERVERS.get(0).bootstrap();
            SERVERS.get(0).start();
        }
        if (addServers(n, true)) {
            reconfigure();
        }
    }

    public static void startCluster(int n) throws Exception {
        startCluster(n, true);
    }

    public static void restartCluster(int n) throws Exception {
        startCluster(n, false);
    }

    static boolean addServers(int n, boolean start) throws Exception {
        boolean needReconfigure = false;

        while (SERVERS.size() < n) {
            int i = SERVERS.size();
            if (i != 0) {
                needReconfigure = true;
            }
            KeyValueServer server = buildServer(i);

            SERVERS.add(server);

            if (start) {
                server.start();
            }
        }

        return needReconfigure;
    }

    protected static KeyValueServer buildServer(int i) {
        Replica local = Replica.fromString("localhost:" + (10000 + i));

        File baseDir = new File(TEMPDIR, "" + i);

        KeyValueConfig config = new KeyValueConfig();
        config.redisEndpoint = HostAndPort.fromParts("", 6379 + i + 1);
        config.protobufEndpoint = HostAndPort.fromParts("", 2000 + i + 1);
        config.httpPort = 9990 + i;
        config.gossip = new GossipConfig();
        config.gossip.serviceId = "keyvalue";
        config.gossip.nodeId = "node" + i;
        config.gossip.broadcastAddress = HostAndPort.fromParts("", 2100 + i + 1);
        config.gossip.protobufEndpoint = HostAndPort.fromParts("", 2200 + i + 1);

        KeyValueServer server = new KeyValueServer(baseDir, local, config);
        return server;
    }

    static void reconfigure() {
        KeyValueServer leader = null;
        List<String> serverKeys = Lists.newArrayList();
        for (int i = 0; i < SERVERS.size(); i++) {
            KeyValueServer server = SERVERS.get(i);
            if (server == null) {
                continue;
            }
            if (server.isLeader()) {
                leader = server;
            }
            serverKeys.add(server.getRaftServerKey());
        }

        if (leader == null) {
            throw new IllegalStateException("Could not find leader");
        }
        leader.reconfigure(serverKeys);
    }

    public static void stopServers() throws Exception {
        log.info("STOPPING ALL SERVERS");

        while (SERVERS.size() > 0) {
            int index = SERVERS.size() - 1;
            KeyValueServer server = SERVERS.get(index);

            if (server != null) {
                server.stop();
            }
            SERVERS.remove(index);
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        stopServers();

        // Thread.sleep(5000);

        TestUtils.rmdir(TEMPDIR);
        TEMPDIR = null;
    }

    static long nextStoreId = 1;

    protected long newStoreId() {
        return nextStoreId++;
    }

}
