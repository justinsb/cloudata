package com.cloudata.keyvalue;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudata.keyvalue.KeyValueClient.ClusterState;
import com.google.common.collect.Lists;

public class StartStopTest extends HttpTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        resetState();
    }

    @Test
    public void testRestartCluster() throws Exception {
        long storeId = newStoreId();
        int n = 20;

        startCluster(1);

        String url = SERVERS.get(0).getHttpUrl();
        
        ClusterState clusterState = ClusterState.fromSeeds(url);
        KeyValueClient client = new KeyValueClient(clusterState);

        checkWrite(client, storeId, n);
        checkRead(client, storeId, n);

        stopServers();

        restartCluster(1);

        checkRead(client, storeId, n);
    }

}
