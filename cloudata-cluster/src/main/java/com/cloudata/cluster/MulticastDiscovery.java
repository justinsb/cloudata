package com.cloudata.cluster;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.util.InetSocketAddresses;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

public class MulticastDiscovery {
  private static final Logger log = LoggerFactory.getLogger(MulticastDiscovery.class);

  // public static MulticastDiscovery build(ClusterState clusterState, HostAndPort local) throws IOException {
  // DatagramSocket sendSocket = new DatagramSocket();
  // sendSocket.setBroadcast(true);
  //
  // String host = local.getHostText();
  // InetAddress localAddress = InetAddresses.forString(host);
  // int port = local.getPort();
  //
  // MulticastSocket receiveSocket = new MulticastSocket(port);
  // receiveSocket.joinGroup(localAddress);
  // receiveSocket.setSoTimeout(1000);
  //
  // ArrayList<InetAddress> broadcastAddresses = Lists.newArrayList();
  // broadcastAddresses.add(localAddress);
  //
  // return new MulticastDiscovery(clusterState, local, broadcastAddresses, sendSocket, receiveSocket);
  // }
  //
  // private MulticastDiscovery(ClusterState clusterState, HostAndPort localSocketAddress,
  // ArrayList<InetAddress> broadcastAddresses, DatagramSocket sendSocket, DatagramSocket receiveSocket)
  // throws SocketException {
  // super(clusterState, localSocketAddress, broadcastAddresses, sendSocket, receiveSocket);
  // }

  public static CastingListenerService buildListener(ClusterState clusterState, HostAndPort local) throws IOException {
    InetSocketAddress socketAddress = InetSocketAddresses.fromHostAndPort(local);

    MulticastSocket receiveSocket = new MulticastSocket(socketAddress.getPort());
    receiveSocket.joinGroup(socketAddress.getAddress());

    return new CastingListenerService(clusterState, receiveSocket);

  }

  public static CastingSenderService buildSender(ScheduledExecutorService executor, HostAndPort broadcastAddress)
      throws SocketException {
    DatagramSocket sendSocket = new DatagramSocket();
    sendSocket.setBroadcast(true);

    ArrayList<InetSocketAddress> broadcastAddresses = Lists.newArrayList();
    broadcastAddresses.add(InetSocketAddresses.fromHostAndPort(broadcastAddress));

    return new CastingSenderService(executor, sendSocket, broadcastAddresses);
  }

}
