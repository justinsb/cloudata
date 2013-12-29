package com.cloudata.structured;

import java.io.File;
import java.util.EnumSet;
import java.util.List;

import javax.servlet.DispatcherType;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.Replica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.structured.web.WebModule;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;

public class StructuredServer {

    private static final Logger log = LoggerFactory.getLogger(StructuredServer.class);

    final File baseDir;
    final int httpPort;
    private final Replica local;
    private final List<Replica> peers;
    private RaftService raft;
    private Server jetty;

    public StructuredServer(File baseDir, Replica local, List<Replica> peers, int httpPort) {
        this.baseDir = baseDir;
        this.local = local;
        this.peers = peers;
        this.httpPort = httpPort;
    }

    public synchronized void start() throws Exception {
        if (raft != null || jetty != null) {
            throw new IllegalStateException();
        }

        File logDir = new File(baseDir, "logs");
        File stateDir = new File(baseDir, "state");

        logDir.mkdirs();
        stateDir.mkdirs();

        StructuredStateMachine stateMachine = new StructuredStateMachine();

        ClusterConfig config = ClusterConfig.from(local, peers);
        this.raft = RaftService.newBuilder(config).logDir(logDir).timeout(300).build(stateMachine);

        stateMachine.init(raft, stateDir);

        raft.startAsync().awaitRunning();

        // final String baseUri = getHttpUrl();

        Injector injector = Guice.createInjector(new StructuredStoreModule(stateMachine), new WebModule());

        // ResourceConfig rc = new PackagesResourceConfig(WebModule.class.getPackage().getName());
        // IoCComponentProviderFactory ioc = new GuiceComponentProviderFactory(rc, injector);
        //
        // this.selector = GrizzlyServerFactory.create(baseUri, rc, ioc);
        //

        this.jetty = new Server(httpPort);

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");

        FilterHolder filterHolder = new FilterHolder(injector.getInstance(GuiceFilter.class));
        context.addFilter(filterHolder, "*", EnumSet.of(DispatcherType.REQUEST));

        jetty.setHandler(context);

        jetty.start();
    }

    public String getHttpUrl() {
        return "http://localhost:" + httpPort + "/";
    }

    public static void main(String... args) throws Exception {
        final int port = Integer.parseInt(args[0]);

        Replica local = Replica.fromString("localhost:" + (10000 + port));
        List<Replica> members = Lists.newArrayList(Replica.fromString("localhost:10001"),
                Replica.fromString("localhost:10002"), Replica.fromString("localhost:10003"));
        members.remove(local);

        File baseDir = new File(args[0]);
        int httpPort = (9990 + port);

        final StructuredServer server = new StructuredServer(baseDir, local, members, httpPort);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    server.stop();
                } catch (Exception e) {
                    log.warn("Error shutting down HTTP server", e);
                }
            }
        });
    }

    public synchronized void stop() throws Exception {
        if (jetty != null) {
            jetty.stop();
            jetty = null;
        }

        if (raft != null) {
            raft.stopAsync().awaitTerminated();
            raft = null;
        }
    }

}
