package com.cloudata.keyvalue;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.Executors;

import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.ProtobufServer;
import com.cloudata.cluster.ClusterService;
import com.cloudata.keyvalue.protobuf.KeyValueProtobufEndpoint;
import com.cloudata.keyvalue.redis.RedisEndpoint;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.web.WebModule;
import com.cloudata.services.CompoundService;
import com.cloudata.services.JettyService;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.Service;

public class KeyValueServer extends CompoundService {
    private static final Logger log = LoggerFactory.getLogger(KeyValueServer.class);

    final File baseDir;
    private final Replica local;
    private final RaftService raft;

    private final KeyValueStateMachine stateMachine;

    private final KeyValueConfig config;

    public KeyValueServer(File baseDir, Replica local, KeyValueConfig config) {
        this.baseDir = baseDir;
        this.local = local;

        this.config = config.deepCopy();

        File logDir = new File(baseDir, "logs");
        File stateDir = new File(baseDir, "state");

        logDir.mkdirs();
        stateDir.mkdirs();

        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors
                .newCachedThreadPool(new DefaultThreadFactory("pool-worker-keyvalue")));
        this.stateMachine = new KeyValueStateMachine(executor, stateDir);

        ClusterConfig clusterConfig = ClusterConfig.from(local);
        this.raft = RaftService.newBuilder(clusterConfig).logDir(logDir).timeout(300).build(stateMachine);
    }

    public void bootstrap() {
        Membership membership = Membership.newBuilder().addMembers(local.getKey()).build();
        this.raft.bootstrap(membership);
    }

    public String getHttpUrl() {
        return "http://localhost:" + config.httpPort + "/";
    }

    public static void main(String... args) throws Exception {
        final int port = Integer.parseInt(args[0]);

        Replica local = Replica.fromString("localhost:" + (10000 + port));

        KeyValueConfig config = new KeyValueConfig();

        File baseDir = new File(args[0]);
        config.httpPort = (9990 + port);
        int redisPort = 6379 + port;
        int protobufPort = 2000 + port;

        config.redisEndpoint = HostAndPort.fromParts("", redisPort);
        config.protobufEndpoint = HostAndPort.fromParts("", protobufPort);

        final KeyValueServer server = new KeyValueServer(baseDir, local, config);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    server.stop();
                } catch (Exception e) {
                    log.error("Error stopping server", e);
                }
            }
        });
    }

    public HostAndPort getRedisSocketAddress() {
        return config.redisEndpoint;
    }

    public HostAndPort getProtobufSocketAddress() {
        return config.protobufEndpoint;
    }

    public String getRaftServerKey() {
        return raft.getServerKey();
    }

    public void reconfigure(List<String> servers) {
        RaftMembership oldMembership = raft.getClusterMembership();
        RaftMembership newMembership = new RaftMembership(-1, servers);
        ListenableFuture<Boolean> future = raft.setConfiguration(oldMembership, newMembership);
        Boolean result = Futures.getUnchecked(future);
        if (!Boolean.TRUE.equals(result)) {
            throw new IllegalStateException();
        }
    }

    public boolean isLeader() {
        return raft.isLeader();
    }

    @Override
    protected List<com.google.common.util.concurrent.Service> buildServices() {
        Injector injector = Guice.createInjector(new KeyValueModule(stateMachine), new WebModule());

        List<com.google.common.util.concurrent.Service> services = Lists.newArrayList();

        services.add(new StateMachineService(stateMachine, raft));

        services.add(raft);

        JettyService jetty = injector.getInstance(JettyService.class);
        jetty.init(config.httpPort);
        services.add(jetty);

        if (config.redisEndpoint != null) {
            long storeId = 1;
            RedisServer redisServer = new RedisServer(stateMachine, storeId);

            RedisEndpoint redisEndpoint = new RedisEndpoint(config.redisEndpoint, redisServer);
            services.add(redisEndpoint);
        }

        if (config.protobufEndpoint != null) {
            ProtobufServer protobufServer = new ProtobufServer(config.protobufEndpoint);

            KeyValueProtobufEndpoint endpoint = injector.getInstance(KeyValueProtobufEndpoint.class);
            Service service = KeyValueProtocol.KeyValueService.newReflectiveService(endpoint);

            protobufServer.addService(service);

            services.add(protobufServer);
        }

        if (config.gossip != null) {
            ClusterService cluster = new ClusterService(config.gossip);
            services.add(cluster);

            services.add(new RepairService(raft, cluster));
        }

        return services;
    }

}
