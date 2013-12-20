package com.cloudata.files.fs;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

@Singleton
public class FsContext {
    private static final Logger log = LoggerFactory.getLogger(FsContext.class);

    @Inject
    FsClient fsClient;

    final ListeningExecutorService executor;

    public ListeningExecutorService getExecutor() {
        return executor;
    }

    public FsContext() {
        executor = buildExecutor();
    }

    private ListeningExecutorService buildExecutor() {
        int corePoolSize = 4;
        int maximumPoolSize = 32;
        long keepAliveTime = 60;
        BlockingQueue<Runnable> workQueue = new SynchronousQueue<Runnable>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime,
                TimeUnit.SECONDS, workQueue);

        return MoreExecutors.listeningDecorator(executor);
    }

}
