package com.cloudata.keyvalue;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.AfterClass;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.proto.RaftEntry.ConfigTimeouts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.TestUtils;
import com.cloudata.TimeSpan;
import com.cloudata.snapshots.LocalSnapshotStorage;
import com.cloudata.snapshots.SnapshotStorage;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.net.HostAndPort;

public class IntegrationTestBase {
  private static final Logger log = LoggerFactory.getLogger(IntegrationTestBase.class);

  protected static List<KeyValueServer> SERVERS;
  static File TEMPDIR;

  static {
    resetState();
  }

  public static void resetState() {
    SERVERS = Lists.newArrayList();
    if (TEMPDIR != null) {
      TestUtils.rmdir(TEMPDIR);
      TEMPDIR = null;
    }
    TEMPDIR = Files.createTempDir();
  }

  static void startCluster(int n, boolean bootstrap) throws Exception {
    if (bootstrap) {
      addServers(1, false);
       SERVERS.get(0).bootstrap();
      SERVERS.get(0).startAsync().awaitRunning();
    }
    if (addServers(n, true)) {
      reconfigure();
    }
  }

  public static void startCluster(int n) throws Exception {
    startCluster(n, true);
  }

  public static void restartCluster(int n) throws Exception {
    startCluster(n, false);
  }

  static boolean addServers(int n, boolean start) throws Exception {
    boolean needReconfigure = false;

    while (SERVERS.size() < n) {
      int i = SERVERS.size();
      if (i != 0) {
        needReconfigure = true;
      }
      KeyValueServer server = buildServer(i, SERVERS.subList(0, i));

      SERVERS.add(server);

      if (start) {
        server.startAsync().awaitRunning();
      }
    }

    return needReconfigure;
  }

  protected static KeyValueServer buildServer(int i, Iterable<KeyValueServer> seedMembers) throws IOException {
    Replica local = Replica.fromString("localhost:" + (10000 + i));

    File baseDir = new File(TEMPDIR, "" + i);

    KeyValueConfig config = new KeyValueConfig();
    config.redisEndpoint = HostAndPort.fromParts("", 6379 + i + 1);
    config.protobufEndpoint = HostAndPort.fromParts("", 2000 + i + 1);
    config.httpPort = 9990 + i;
    
    /*
    config.gossip = new GossipConfig();
    config.gossip.serviceId = "keyvalue";
    config.gossip.nodeId = "node" + i;
//    config.gossip.broadcastAddress = HostAndPort.fromParts("225.1.1.1", 2100 + i + 1);
    config.gossip.protobufEndpoint = HostAndPort.fromParts("", 2200 + i + 1);
    */
    
    List<Replica> allMembers = Lists.newArrayList();
    for (KeyValueServer seedMember : seedMembers) {
      if (seedMember == null) continue;
      allMembers.add(seedMember.getReplica());
    }
    ConfigTimeouts timeouts = ClusterConfig.buildDefaultTimeouts();
    ClusterConfig seedConfig = new ClusterConfig(local, allMembers, timeouts);
    config.seedConfig = seedConfig;
    
    SnapshotStorage snapshotStore = new LocalSnapshotStorage(new File(TEMPDIR, "snapshots"));
    KeyValueServer server = new KeyValueServer(baseDir, local, config, snapshotStore);
    return server;
  }

  static void sanityCheck() {
    KeyValueServer leader = findLeader();
  }

  static KeyValueServer getLeader() {
    for (int i = 0; i < 10; i++) {
      if (i != 0) {
        log.warn("Unable to find leader; will sleep before retry");
        TimeSpan.seconds(1).sleep();

      }
      KeyValueServer leader = findLeader();

      if (leader != null) {
        return leader;
      }
    }

    log.warn("Could not find leader");
    for (int i = 0; i < SERVERS.size(); i++) {
      KeyValueServer server = SERVERS.get(i);

      log.warn("server {}: {}", i, server);
    }

    throw new IllegalStateException("Could not find leader");
  }

  private static KeyValueServer findLeader() {
    KeyValueServer leader = null;
    for (KeyValueServer server : SERVERS) {
      if (server == null) {
        continue;
      }
      if (server.isLeader()) {
        if (leader != null) {
          throw new IllegalStateException("Found multiple leaders");
        }
        leader = server;
      }
    }
    return leader;
  }

  static void reconfigure() throws RaftException {
    int attempt = 0;
    int maxAttempts = 10;
    while (true) {
      attempt++;
      final KeyValueServer leader = getLeader();
      List<String> serverKeys = Lists.newArrayList();
      for (int i = 0; i < SERVERS.size(); i++) {
        KeyValueServer server = SERVERS.get(i);
        if (server == null) {
          continue;
        }
        serverKeys.add(server.getRaftServerKey());
      }
      try {
        leader.reconfigure(serverKeys);
        return;
      } catch (NotLeaderException e) {
        if (attempt > maxAttempts) {
          throw e;
        }
        log.warn("NotLeader during reconfigure; will retry");
      }
    }
  }

  public static void stopServers() throws Exception {
    log.info("STOPPING ALL SERVERS");

    while (SERVERS.size() > 0) {
      int index = SERVERS.size() - 1;
      KeyValueServer server = SERVERS.get(index);

      if (server != null) {
        server.stopAsync().awaitTerminated();
      }
      SERVERS.remove(index);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    stopServers();

    // Thread.sleep(5000);

    TestUtils.rmdir(TEMPDIR);
    TEMPDIR = null;
  }

  static long nextStoreId = 1;

  protected long newStoreId() {
    return nextStoreId++;
  }

}
