package com.cloudata.keyvalue;

import java.net.ConnectException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.KeyValueClient.ClusterState;
import com.google.protobuf.ByteString;

public class ClusterTest extends HttpTestBase {

    private static final Logger log = LoggerFactory.getLogger(ClusterTest.class);

    @Before
    public void beforeEachTest() throws Exception {
        resetState();
        startCluster(3);
    }

    @After
    public void afterEachTest() throws Exception {
        stopServers();
    }

    @Test
    public void testExpansion() throws Exception {
        String url = SERVERS.get(0).getHttpUrl();

        long storeId = newStoreId();

        KeyValueClient client = new KeyValueClient(ClusterState.fromSeeds(url));

        int n = 10;

        checkWrite(client, storeId, n);

        Thread.sleep(1000);

        url = SERVERS.get(1).getHttpUrl();
        client = new KeyValueClient(ClusterState.fromSeeds(url));

        checkRead(client, storeId, n);

        addServers(5, true);

        reconfigure();

        Thread.sleep(1000);

        url = SERVERS.get(4).getHttpUrl();
        client = new KeyValueClient(ClusterState.fromSeeds(url));

        checkRead(client, storeId, n);
    }

    @Test
    public void testShutdownAndRestart() throws Exception {
        String url = SERVERS.get(0).getHttpUrl();

        long storeId = newStoreId();

        KeyValueClient client = new KeyValueClient(ClusterState.fromSeeds(url));

        checkWrite(client, storeId, 5);
        checkRead(client, storeId, 5);

        Thread.sleep(1000);

        System.out.println("STOPPING SERVER 0");
        SERVERS.get(0).stopAsync().awaitTerminated();

        Thread.sleep(1000);

        url = SERVERS.get(0).getHttpUrl();
        client = new KeyValueClient(ClusterState.fromSeeds(url));

        try {
            client.read(storeId, ByteString.EMPTY);
            Assert.fail("Expected exception");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof ConnectException);
        }

        url = SERVERS.get(1).getHttpUrl();
        client = new KeyValueClient(ClusterState.fromSeeds(url));

        checkRead(client, storeId, 5);
        checkWrite(client, storeId, 10);
        checkRead(client, storeId, 10);

        System.out.println("STARTING SERVER 0");
        SERVERS.set(0, buildServer(0, SERVERS.subList(0, 0)));
        SERVERS.get(0).startAsync().awaitRunning();

        Thread.sleep(1000);

        url = SERVERS.get(0).getHttpUrl();
        client = new KeyValueClient(ClusterState.fromSeeds(url));

        checkRead(client, storeId, 10);
        checkWrite(client, storeId, 10);
        checkRead(client, storeId, 10);
    }

    @Test
    public void testShutdownAndReplace() throws Exception {
        long storeId = newStoreId();

        String url = SERVERS.get(0).getHttpUrl();
        KeyValueClient client = new KeyValueClient(ClusterState.fromSeeds(url));

        checkWrite(client, storeId, 5);
        checkRead(client, storeId, 5);

        System.out.println("STOPPING SERVER 0");
        SERVERS.get(0).stopAsync().awaitTerminated();

        Thread.sleep(1000);

        url = SERVERS.get(0).getHttpUrl();
        client = new KeyValueClient(ClusterState.fromSeeds(url));

        try {
            client.read(storeId, ByteString.EMPTY);
            Assert.fail("Expected exception");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof ConnectException);
        }

        url = SERVERS.get(1).getHttpUrl();
        client = new KeyValueClient(ClusterState.fromSeeds(url));

        checkRead(client, storeId, 5);
        checkWrite(client, storeId, 10);
        checkRead(client, storeId, 10);

        System.out.println("STARTING SERVER 3, REPLACEMENT FOR #0");
        SERVERS.set(0, null);
        SERVERS.add(buildServer(3, SERVERS.subList(0, 3)));
        SERVERS.get(3).startAsync().awaitRunning();

        reconfigure();

        Thread.sleep(1000);

        url = SERVERS.get(3).getHttpUrl();
        client = new KeyValueClient(ClusterState.fromSeeds(url));

        checkRead(client, storeId, 10);
        checkWrite(client, storeId, 10);
        checkRead(client, storeId, 10);
    }

    @Test
    public void testShutdownAndReplaceAll() throws Exception {
        long storeId = newStoreId();

        String url = SERVERS.get(0).getHttpUrl();
        KeyValueClient client = new KeyValueClient(ClusterState.fromSeeds(url));

        checkWrite(client, storeId, 5);
        checkRead(client, storeId, 5);

        for (int i = 0; i < 3; i++) {
            SERVERS.get(i).stopAsync().awaitTerminated();

            Thread.sleep(1000);

            url = SERVERS.get(i).getHttpUrl();
            client = new KeyValueClient(ClusterState.fromSeeds(url));

            try {
                client.read(storeId, ByteString.EMPTY);
                Assert.fail("Expected exception");
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof ConnectException);
            }

            url = SERVERS.get(i + 1).getHttpUrl();
            client = new KeyValueClient(ClusterState.fromSeeds(url));

            checkRead(client, storeId, 5);

            log.info("STARTING REPLACEMENT SERVER #{}", (i + 3));
            SERVERS.set(i, null);
            SERVERS.add(buildServer(i + 3, SERVERS.subList(0, i+3)));
            SERVERS.get(i + 3).startAsync().awaitRunning();

            reconfigure();

            Thread.sleep(1000);

            url = SERVERS.get(i + 3).getHttpUrl();
            client = new KeyValueClient(ClusterState.fromSeeds(url));

            checkRead(client, storeId, 5);
        }

        url = SERVERS.get(3).getHttpUrl();
        client = new KeyValueClient(ClusterState.fromSeeds(url));

        checkWrite(client, storeId, 5);
        checkRead(client, storeId, 5);
    }

    @Test
    public void testLeaderFailover() throws Exception {
        long storeId = newStoreId();

        String url = SERVERS.get(0).getHttpUrl();
        KeyValueClient client = new KeyValueClient(ClusterState.fromSeeds(url));

        int n = 20;

        checkWrite(client, storeId, n);

        Thread.sleep(1000);

        SERVERS.get(0).stopAsync().awaitTerminated();

        Thread.sleep(1000);

        url = SERVERS.get(1).getHttpUrl();
        client = new KeyValueClient(ClusterState.fromSeeds(url));

        checkRead(client, storeId, n);
    }

    @Test
    public void testCrossRead() throws Exception {
        String url = SERVERS.get(0).getHttpUrl();

        long storeId = newStoreId();

        KeyValueClient client = new KeyValueClient(ClusterState.fromSeeds(url));

        int n = 20;

        checkWrite(client, storeId, n);

        Thread.sleep(1000);

        for (int j = 0; j < SERVERS.size(); j++) {
            url = SERVERS.get(j).getHttpUrl();
            client = new KeyValueClient(ClusterState.fromSeeds(url));

            checkRead(client, storeId, n);
        }
    }

}
