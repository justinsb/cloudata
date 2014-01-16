package com.cloudata.keyvalue;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;

public class KeyValueModule extends AbstractModule {

    private final KeyValueStateMachine keyValueStateMachine;

    public KeyValueModule(KeyValueStateMachine keyValueStateMachine) {
        this.keyValueStateMachine = keyValueStateMachine;
    }

    @Override
    protected void configure() {
        bind(KeyValueStateMachine.class).toInstance(keyValueStateMachine);
        ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(Executors
                .newCachedThreadPool(new DefaultThreadFactory("pool-executor-keyvalue")));
        bind(ListeningExecutorService.class).toInstance(listeningExecutorService);
    }

}
