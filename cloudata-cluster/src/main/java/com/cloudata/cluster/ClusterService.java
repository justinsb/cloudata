package com.cloudata.cluster;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import com.cloudata.ProtobufServer;
import com.cloudata.cluster.ClusterProtocol.BroadcastMessage;
import com.cloudata.cluster.ClusterProtocol.BroadcastMessage.Builder;
import com.cloudata.cluster.endpoint.ClusterEndpoint;
import com.cloudata.services.CompoundService;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.Service;
import com.google.protobuf.ByteString;

public class ClusterService extends CompoundService {

  final GossipConfig config;

  final ClusterState clusterState;

  private final ScheduledExecutorService executor;

  // public static void main(String[] args) throws IOException {
  // Random random = new Random();
  // int protobufPort = random.nextInt(10000) + 300000;
  // InetSocketAddress protobufSocketAddress = new InetSocketAddress(protobufPort);
  // if (protobufSocketAddress != null) {
  // ProtobufServer protobufServer = new ProtobufServer(protobufSocketAddress);
  //
  // ClusterEndpoint endpoint = injector.getInstance(ClusterEndpoint.class);
  // Service service = KeyValueProtocol.KeyValueService.newReflectiveService(endpoint);
  //
  // protobufServer.addService(service);
  //
  // this.protobufServer.start();
  // }
  //
  // InetAddress address = InetAddresses.forString(args[0]);
  // String id = args[1];
  // int port = 1234;
  //
  // // BroadcastDiscovery discovery = BroadcastDiscovery.build(new InetSocketAddress(address, port));
  // CastingDiscovery discovery = MulticastDiscovery.build(new InetSocketAddress(address, port));
  //
  // discovery.message = BroadcastMessage.newBuilder().setId(ByteString.copyFromUtf8(id)).build();
  //
  // discovery.run();
  // }

  public ClusterService(GossipConfig config, ScheduledExecutorService executor) {
    this.config = config.deepCopy();
    this.clusterState = new ClusterState();
    this.executor = executor;
  }

  @Override
  protected List<Service> buildServices() throws IOException {
    List<Service> services = Lists.newArrayList();

    if (config.protobufEndpoint != null) {
      ProtobufServer protobufServer = new ProtobufServer(config.protobufEndpoint);

      ClusterEndpoint endpoint = new ClusterEndpoint(clusterState);

      protobufServer.addService(ClusterProtocol.ClusterRpcService.newReflectiveService(endpoint));

      services.add(protobufServer);
    }

    if (config.broadcastAddress != null) {
      CastingSenderService discoverySender = MulticastDiscovery.buildSender(executor, config.broadcastAddress);

      {
        Builder b = BroadcastMessage.newBuilder();
        if (config.protobufEndpoint != null) {
          String host = config.protobufEndpoint.getHostText();
          if (!Strings.isNullOrEmpty(host)) {
            InetAddress address = InetAddresses.forString(host);
            b.setAddress(ByteString.copyFrom(address.getAddress()));
          }
          b.setPort(config.protobufEndpoint.getPort());
        }
        if (config.serviceId != null) {
          b.setService(config.serviceId);
        }

        if (config.nodeId != null) {
          b.setId(config.nodeId);
        }
        discoverySender.setMessage(b.build());
      }

      services.add(discoverySender);

      services.add(MulticastDiscovery.buildListener(clusterState, config.broadcastAddress));
    }

    return services;
  }

  public List<ClusterPeer> choosePeers(List<ClusterId> aliveMembers, List<ClusterId> deadMembers, int pickCount) {
    return clusterState.choosePeers(aliveMembers, deadMembers, pickCount);
  }

}
