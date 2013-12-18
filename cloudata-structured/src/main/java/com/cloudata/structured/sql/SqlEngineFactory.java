package com.cloudata.structured.sql;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.cloudata.structured.StructuredStateMachine;
import com.cloudata.structured.StructuredStore;

@Singleton
public class SqlEngineFactory {

    @Inject
    StructuredStateMachine stateMachine;

    final ExecutorService executor = Executors.newCachedThreadPool();

    public SqlEngine get(long storeId) {
        StructuredStore structuredStore = stateMachine.getStructuredStore(storeId);

        return new SqlEngine(structuredStore, executor);
    }

}
