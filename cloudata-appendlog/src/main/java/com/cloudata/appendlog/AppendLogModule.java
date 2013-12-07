package com.cloudata.appendlog;

import com.google.inject.AbstractModule;

public class AppendLogModule extends AbstractModule {

    private final AppendLogStore appendLogStore;

    public AppendLogModule(AppendLogStore appendLogStore) {
        this.appendLogStore = appendLogStore;
    }

    @Override
    protected void configure() {
        bind(AppendLogStore.class).toInstance(appendLogStore);
    }

}
