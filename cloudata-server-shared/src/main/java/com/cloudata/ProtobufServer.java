package com.cloudata;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.InetSocketAddress;
import java.util.List;

import org.robotninjas.protobuf.netty.server.RpcServer;

import com.cloudata.services.CompoundService;
import com.cloudata.services.ExecutorServiceWrapper;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Service;

public class ProtobufServer extends CompoundService {
    private final HostAndPort hostAndPort;
    private final NioEventLoopGroup eventLoopGroup;
    private final RpcServer rpcServer;

    public ProtobufServer(HostAndPort hostAndPort) {
        this.hostAndPort = hostAndPort;
        this.eventLoopGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("pool-protobuf"));
        this.rpcServer = new RpcServer(eventLoopGroup, new InetSocketAddress(hostAndPort.getHostText(),
                hostAndPort.getPort()));
    }

    public void addService(Service service) {
        rpcServer.registerService(service);
    }

    @Override
    protected List<com.google.common.util.concurrent.Service> buildServices() {
        List<com.google.common.util.concurrent.Service> services = Lists.newArrayList();
        services.add(new ExecutorServiceWrapper(eventLoopGroup));
        services.add(rpcServer);
        return services;
    }

}
