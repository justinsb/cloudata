package com.cloudata.clients.protobuf;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.pool.PoolUtils.adapt;
import static org.apache.commons.pool.impl.GenericKeyedObjectPool.WHEN_EXHAUSTED_FAIL;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.robotninjas.protobuf.netty.client.NettyRpcChannel;
import org.robotninjas.protobuf.netty.client.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;

@Immutable
public class ProtobufRpcClientProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufRpcChannelFactory.class);
    private static final GenericKeyedObjectPool.Config config;

    static {
        config = new GenericKeyedObjectPool.Config();
        config.maxActive = 1;
        config.maxIdle = -1;
        config.testOnBorrow = true;
        config.testOnReturn = true;
        config.whenExhaustedAction = WHEN_EXHAUSTED_FAIL;
    }

    private final GenericKeyedObjectPool<Object, ListenableFuture<NettyRpcChannel>> connectionPools;

    @Inject
    public ProtobufRpcClientProvider(@Nonnull RpcClient client) {
        ProtobufRpcChannelFactory channelFactory = new ProtobufRpcChannelFactory(client);
        this.connectionPools = new GenericKeyedObjectPool<Object, ListenableFuture<NettyRpcChannel>>(channelFactory,
                config);
    }

    @Nonnull
    public ProtobufRpcClient get(@Nonnull HostAndPort hostAndPort) {
        checkNotNull(hostAndPort);
        return new ProtobufRpcClient(adapt(connectionPools, hostAndPort));
    }

}
