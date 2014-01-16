package com.cloudata.keyvalue;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Executors;

import javax.servlet.DispatcherType;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.ProtobufServer;
import com.cloudata.keyvalue.protobuf.KeyValueProtobufEndpoint;
import com.cloudata.keyvalue.redis.RedisEndpoint;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.web.WebModule;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.google.protobuf.Service;

public class KeyValueServer {
    private static final Logger log = LoggerFactory.getLogger(KeyValueServer.class);

    final File baseDir;
    final int httpPort;
    private final Replica local;
    private final RaftService raft;
    private final SocketAddress redisSocketAddress;
    private RedisEndpoint redisEndpoint;
    private Server jetty;
    private final SocketAddress protobufSocketAddress;

    private ProtobufServer protobufServer;

    private final KeyValueStateMachine stateMachine;

    public KeyValueServer(File baseDir, Replica local, int httpPort, SocketAddress redisSocketAddress,
            SocketAddress protobufSocketAddress) {
        this.baseDir = baseDir;
        this.local = local;
        this.httpPort = httpPort;
        this.redisSocketAddress = redisSocketAddress;
        this.protobufSocketAddress = protobufSocketAddress;

        File logDir = new File(baseDir, "logs");
        File stateDir = new File(baseDir, "state");

        logDir.mkdirs();
        stateDir.mkdirs();

        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors
                .newCachedThreadPool(new DefaultThreadFactory("pool-worker-keyvalue")));
        this.stateMachine = new KeyValueStateMachine(executor, stateDir);

        ClusterConfig config = ClusterConfig.from(local);
        this.raft = RaftService.newBuilder(config).logDir(logDir).timeout(300).build(stateMachine);

        stateMachine.init(raft);
    }

    public void bootstrap() {
        Membership membership = Membership.newBuilder().addMembers(local.getKey()).build();
        this.raft.bootstrap(membership);
    }

    public synchronized void start() throws Exception {
        if (jetty != null) {
            throw new IllegalStateException();
        }

        raft.startAsync().awaitRunning();

        // final String baseUri = getHttpUrl();

        Injector injector = Guice.createInjector(new KeyValueModule(stateMachine), new WebModule());

        // ResourceConfig rc = new PackagesResourceConfig(WebModule.class.getPackage().getName());
        // IoCComponentProviderFactory ioc = new GuiceComponentProviderFactory(rc, injector);

        // this.selector = GrizzlyServerFactory.create(baseUri, rc, ioc);

        this.jetty = new Server(httpPort);

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");

        FilterHolder filterHolder = new FilterHolder(injector.getInstance(GuiceFilter.class));
        context.addFilter(filterHolder, "*", EnumSet.of(DispatcherType.REQUEST));

        jetty.setHandler(context);

        jetty.start();

        if (redisSocketAddress != null) {
            long storeId = 1;
            RedisServer redisServer = new RedisServer(stateMachine, storeId);

            this.redisEndpoint = new RedisEndpoint(redisSocketAddress, redisServer);
            this.redisEndpoint.start();
        }

        if (protobufSocketAddress != null) {
            this.protobufServer = new ProtobufServer(protobufSocketAddress);

            KeyValueProtobufEndpoint endpoint = injector.getInstance(KeyValueProtobufEndpoint.class);
            Service service = KeyValueProtocol.KeyValueService.newReflectiveService(endpoint);

            protobufServer.addService(service);

            this.protobufServer.start();
        }
    }

    public String getHttpUrl() {
        return "http://localhost:" + httpPort + "/";
    }

    public static void main(String... args) throws Exception {
        final int port = Integer.parseInt(args[0]);

        Replica local = Replica.fromString("localhost:" + (10000 + port));

        File baseDir = new File(args[0]);
        int httpPort = (9990 + port);
        int redisPort = 6379 + port;
        int protobufPort = 2000 + port;

        SocketAddress redisSocketAddress = new InetSocketAddress(redisPort);
        SocketAddress protobufSocketAddress = new InetSocketAddress(protobufPort);
        final KeyValueServer server = new KeyValueServer(baseDir, local, httpPort, redisSocketAddress,
                protobufSocketAddress);
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

    public synchronized void stop() throws Exception {
        if (jetty != null) {
            jetty.stop();
            while (true) {
                String state = jetty.getState();
                if (state.equals(AbstractLifeCycle.STOPPED)) {
                    break;
                }
                log.debug("Waiting for jetty; state={}", state);
                Thread.sleep(5000);
            }

            jetty = null;
        }

        if (redisEndpoint != null) {
            try {
                redisEndpoint.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            redisEndpoint = null;
        }

        if (protobufServer != null) {
            try {
                protobufServer.stop();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            protobufServer = null;
        }

        if (stateMachine != null) {
            stateMachine.close();
        }

        if (raft.isRunning()) {
            raft.stopAsync().awaitTerminated();
        }
    }

    public SocketAddress getRedisSocketAddress() {
        return redisSocketAddress;
    }

    public SocketAddress getProtobufSocketAddress() {
        return protobufSocketAddress;
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

}
