package com.cloudata;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.robotninjas.protobuf.netty.server.RpcServer;

import com.google.protobuf.Service;

public class ProtobufServer {
    private final SocketAddress socketAddress;
    private final NioEventLoopGroup eventLoopGroup;
    private final RpcServer rpcServer;

    public ProtobufServer(SocketAddress socketAddress) {
        this.socketAddress = socketAddress;
        this.eventLoopGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("pool-protobuf"));
        this.rpcServer = new RpcServer(eventLoopGroup, socketAddress);
    }

    public com.google.common.util.concurrent.Service start() throws TimeoutException {
        return this.rpcServer.startAsync();
    }

    public void stop() throws TimeoutException {
        this.rpcServer.stopAsync().awaitTerminated(5, TimeUnit.SECONDS);
        this.eventLoopGroup.shutdownGracefully();
    }

    public void addService(Service service) {
        rpcServer.registerService(service);
    }
}
