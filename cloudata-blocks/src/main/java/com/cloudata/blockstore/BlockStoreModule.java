package com.cloudata.blockstore;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import com.cloudata.blockstore.backend.cloud.CloudVolumeProvider;
import com.cloudata.blockstore.backend.cloud.ThreadPools;
import com.cloudata.clients.keyvalue.KeyValueService;
import com.cloudata.clients.keyvalue.redis.RedisKeyValueService;
import com.cloudata.clients.keyvalue.redis.RedisKeyValueStore;
import com.cloudata.files.blobs.BlobService;
import com.cloudata.files.blobs.LocalBlobService;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;

public class BlockStoreModule extends AbstractModule {

    final File basePath;
    final InetSocketAddress redisAddress;

    public BlockStoreModule(File basePath, InetSocketAddress redisAddress) {
        this.basePath = basePath;
        this.redisAddress = redisAddress;
    }

    @Override
    protected void configure() {
        int threads = 16; // TODO: Link to number of cores
        ListeningScheduledExecutorService scheduledExecutor = MoreExecutors.listeningDecorator(Executors
                .newScheduledThreadPool(threads));
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

        ThreadPools executors = new ThreadPools(scheduledExecutor, executor);
        bind(ThreadPools.class).toInstance(executors);

        RedisKeyValueStore redis = new RedisKeyValueStore(redisAddress, executor);
        KeyValueService keyValueService = new RedisKeyValueService(redis);
        bind(KeyValueService.class).toInstance(keyValueService);

        BlobService blobService = new LocalBlobService(executor, basePath);
        bind(BlobService.class).toInstance(blobService);

        bind(VolumeProvider.class).to(CloudVolumeProvider.class).asEagerSingleton();
    }
}
