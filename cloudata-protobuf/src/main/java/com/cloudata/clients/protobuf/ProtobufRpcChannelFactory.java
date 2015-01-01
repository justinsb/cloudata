package com.cloudata.clients.protobuf;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.InetSocketAddress;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.robotninjas.protobuf.netty.client.NettyRpcChannel;
import org.robotninjas.protobuf.netty.client.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

@Immutable
class ProtobufRpcChannelFactory extends BaseKeyedPoolableObjectFactory<Object, ListenableFuture<NettyRpcChannel>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufRpcChannelFactory.class);

    private final RpcClient client;

    public ProtobufRpcChannelFactory(@Nonnull RpcClient client) {
        this.client = checkNotNull(client);
    }

    @Override
    public ListenableFuture<NettyRpcChannel> makeObject(Object key) throws Exception {
        Preconditions.checkArgument(key instanceof HostAndPort);
        HostAndPort hostAndPort = (HostAndPort) key;
        InetSocketAddress socketAddress = new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort());
        return client.connectAsync(socketAddress);
    }

    @Override
    public void destroyObject(Object key, ListenableFuture<NettyRpcChannel> obj) throws Exception {
        if (obj.isDone() && !obj.isCancelled()) {
            obj.get().close();
        } else {
            obj.cancel(false);
        }
    }

    @Override
    public boolean validateObject(Object key, ListenableFuture<NettyRpcChannel> obj) {
        return !obj.isDone() || (obj.isDone() && Futures.getUnchecked(obj).isOpen());
    }

}
