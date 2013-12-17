package com.cloudata.structured;

import com.google.inject.AbstractModule;

public class StructuredStoreModule extends AbstractModule {

    private final StructuredStateMachine stateMachine;

    public StructuredStoreModule(StructuredStateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    @Override
    protected void configure() {
        bind(StructuredStateMachine.class).toInstance(stateMachine);
    }

}
