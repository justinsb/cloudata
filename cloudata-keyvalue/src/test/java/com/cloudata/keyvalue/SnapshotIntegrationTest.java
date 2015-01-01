package com.cloudata.keyvalue;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.robotninjas.barge.proto.RaftEntry.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.KeyValueClient.ClusterState;
import com.cloudata.keyvalue.KeyValueClient.KeyValueEntry;
import com.cloudata.keyvalue.KeyValueClient.KeyValueRecordset;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

public class SnapshotIntegrationTest extends HttpTestBase {
  private static final Logger log = LoggerFactory.getLogger(SnapshotIntegrationTest.class);

    @BeforeClass
    public static void beforeClass() throws Exception {
        resetState();
        startCluster(3);
    }

    @Test
    public void testSetAndGet() throws Exception {
        String url = SERVERS.get(0).getHttpUrl();

        long storeId = newStoreId();

        KeyValueClient client = new KeyValueClient(ClusterState.fromSeeds(url));

        int n = 1200;

        checkWrite(client, storeId, n);

        Thread.currentThread().sleep(10000);
        
        SnapshotInfo snapshotInfo = SERVERS.get(0).getLastSnapshotInfo();
        Assert.assertNotNull(snapshotInfo);
        Assert.assertTrue(snapshotInfo.getLastIncludedIndex() >= 1000);
        
        checkRead(client, storeId, n);
    }

    @Test
    public void testShutdownAndReplaceAll() throws Exception {
      long storeId = newStoreId();

      String url = SERVERS.get(0).getHttpUrl();
      KeyValueClient client = new KeyValueClient(ClusterState.fromSeeds(url));

      checkWrite(client, storeId, 1200);
      checkRead(client, storeId, 1200);

      Thread.currentThread().sleep(10000);
     
      SnapshotInfo snapshotInfo = SERVERS.get(0).getLastSnapshotInfo();
      Assert.assertNotNull(snapshotInfo);
      Assert.assertTrue(snapshotInfo.getLastIncludedIndex() >= 1000);
      
      for (int i = 0; i < 3; i++) {
        SERVERS.get(i).stopAsync().awaitTerminated();

        Thread.sleep(1000);

        // This should find the new leader
        checkRead(client, storeId, 100);

        log.info("STARTING REPLACEMENT SERVER #{}", (i + 3));
        SERVERS.set(i, null);
        SERVERS.add(buildServer(i + 3, SERVERS.subList(0, i + 3)));
        SERVERS.get(i + 3).startAsync().awaitRunning();

        reconfigure();

        Thread.sleep(5000);

        checkRead(client, storeId, 100);
      }

      checkWrite(client, storeId, 100);
      checkRead(client, storeId, 100);
    }

}
