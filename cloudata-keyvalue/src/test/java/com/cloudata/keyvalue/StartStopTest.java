package com.cloudata.keyvalue;

import org.junit.BeforeClass;
import org.junit.Test;

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
        KeyValueClient client = new KeyValueClient(url);

        checkWrite(client, storeId, n);
        checkRead(client, storeId, n);

        stopServers();

        restartCluster(1);

        checkRead(client, storeId, n);
    }

}
