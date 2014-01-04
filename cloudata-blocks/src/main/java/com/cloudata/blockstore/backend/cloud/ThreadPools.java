package com.cloudata.blockstore.backend.cloud;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

public class ThreadPools {
    final ListeningScheduledExecutorService scheduledExecutor;
    final ListeningExecutorService executor;

    public ThreadPools(ListeningScheduledExecutorService scheduledExecutor, ListeningExecutorService executor) {
        this.scheduledExecutor = scheduledExecutor;
        this.executor = executor;
    }

}
