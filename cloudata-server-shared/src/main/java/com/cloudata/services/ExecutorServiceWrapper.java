package com.cloudata.services;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractService;

public class ExecutorServiceWrapper extends AbstractService {
    private final ExecutorService executorService;

    public ExecutorServiceWrapper(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    protected void doStart() {
        notifyStarted();
    }

    @Override
    protected void doStop() {
        try {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
            notifyStopped();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

}
