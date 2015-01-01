package com.cloudata.appendlog;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.journalio.JournalRaftLog;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.robotninjas.barge.rpc.netty.NettyRaftService;
import org.robotninjas.barge.state.ConfigurationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.appendlog.web.WebModule;
import com.cloudata.cluster.ClusterService;
import com.cloudata.cluster.RepairService;
import com.cloudata.services.CompoundService;
import com.cloudata.services.JettyService;
import com.cloudata.snapshots.LocalSnapshotStorage;
import com.cloudata.snapshots.SnapshotStorage;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class AppendLogServer extends CompoundService {
    private static final Logger log = LoggerFactory.getLogger(AppendLogServer.class);

    final File baseDir;
    final int httpPort;
    private final Replica local;
    private RaftService raft;

    final AppendLogConfig config;

    final AppendLogStore stateMachine;

    public AppendLogServer(File baseDir, Replica local, AppendLogConfig config, SnapshotStorage snapshotStorage) {
      Preconditions.checkNotNull(config);
      Preconditions.checkNotNull(config.seedConfig);

      
        this.baseDir = baseDir;
        this.local = local;
        

        this.config = config.deepCopy();

        this.httpPort = config.httpPort;

        File logDir = new File(baseDir, "logs");
        File stateDir = new File(baseDir, "state");

        logDir.mkdirs();
        stateDir.mkdirs();

        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors
            .newCachedThreadPool(new DefaultThreadFactory("pool-worker-keyvalue")));
        this.stateMachine = new AppendLogStore(executor, stateDir, snapshotStorage);

        
        {
          JournalRaftLog.Builder logBuilder = new JournalRaftLog.Builder();
          logBuilder.logDirectory = logDir;
          logBuilder.stateMachine = stateMachine;
          logBuilder.config = ConfigurationState.buildSeed(config.seedConfig);
          
          NettyRaftService.Builder b = NettyRaftService.newBuilder();
//          b.seedConfig = config.seedConfig;
          b.log = logBuilder;
          // b.listener = groupOfCounters;
          
          this.raft = b.build();
        }
    }

    public void bootstrap() {
        Membership membership = Membership.newBuilder().addMembers(local.getKey()).build();
        this.raft.bootstrap(membership);
    }


    @Override
    protected List<com.google.common.util.concurrent.Service> buildServices() {
      Injector injector = Guice.createInjector(new AppendLogModule(stateMachine), new WebModule());

      List<com.google.common.util.concurrent.Service> services = Lists.newArrayList();

      services.add(raft);

      JettyService jetty = injector.getInstance(JettyService.class);
      jetty.init(config.httpPort);
      services.add(jetty);

      if (config.gossip != null) {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(0, new DefaultThreadFactory(
            "pool-gossip-workers"));

        ClusterService cluster = new ClusterService(config.gossip, executor);
        services.add(cluster);

        services.add(new RepairService(raft, cluster, executor));
      }

      return services;
    }

    String getHttpUrl() {
        return "http://localhost:" + httpPort + "/";
    }

    public static void main(String... args) throws Exception {
      
      final int port = Integer.parseInt(args[0]);

      Replica local = Replica.fromString("localhost:" + (10000 + port));

      AppendLogConfig config = new AppendLogConfig();

      File baseDir = new File(args[0]);
      config.httpPort = (9990 + port);

      SnapshotStorage snapshotStore = new LocalSnapshotStorage(new File(baseDir, "snapshots"));
      
        final AppendLogServer server = new AppendLogServer(baseDir, local, config, snapshotStore);
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

}
