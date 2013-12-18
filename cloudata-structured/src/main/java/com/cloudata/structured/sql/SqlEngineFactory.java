package com.cloudata.structured.sql;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.structured.StructuredStateMachine;
import com.cloudata.structured.StructuredStore;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

@Singleton
public class SqlEngineFactory {
    private static final Logger log = LoggerFactory.getLogger(SqlEngineFactory.class);

    @Inject
    StructuredStateMachine stateMachine;

    final ExecutorService executor = Executors.newCachedThreadPool();

    final LoadingCache<Long, SqlEngine> sqlEngineCache;

    public SqlEngineFactory() {
        SqlEngineLoader loader = new SqlEngineLoader();
        this.sqlEngineCache = CacheBuilder.newBuilder().recordStats().build(loader);
    }

    public SqlEngine get(long storeId) {
        try {
            return sqlEngineCache.get(storeId);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Immutable
    final class SqlEngineLoader extends CacheLoader<Long, SqlEngine> {

        @Override
        public SqlEngine load(@Nonnull Long id) throws Exception {
            checkNotNull(id);
            try {
                StructuredStore structuredStore = stateMachine.getStructuredStore(id);

                return new SqlEngine(structuredStore, executor);
            } catch (Exception e) {
                log.warn("Error building SqlEngine", e);
                throw e;
            }
        }
    }
}
