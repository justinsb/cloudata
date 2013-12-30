package com.cloudata.blockstore;

import com.google.inject.AbstractModule;

public class KeyValueModule extends AbstractModule {

    private final KeyValueStateMachine keyValueStateMachine;

    public KeyValueModule(KeyValueStateMachine keyValueStateMachine) {
        this.keyValueStateMachine = keyValueStateMachine;
    }

    @Override
    protected void configure() {
        bind(KeyValueStateMachine.class).toInstance(keyValueStateMachine);
    }

}
