package com.cloudata.cluster;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;

public class MulticastDiscovery extends CastingDiscovery {

    private static final Logger log = LoggerFactory.getLogger(MulticastDiscovery.class);

    public static MulticastDiscovery build(ClusterState clusterState, HostAndPort local) throws IOException {
        DatagramSocket sendSocket = new DatagramSocket();
        sendSocket.setBroadcast(true);

        InetAddress localAddress = InetAddresses.forString(local.getHostText());
        int port = local.getPort();

        MulticastSocket receiveSocket = new MulticastSocket(port);
        receiveSocket.joinGroup(localAddress);
        receiveSocket.setSoTimeout(1000);

        ArrayList<InetAddress> broadcastAddresses = Lists.newArrayList();
        broadcastAddresses.add(localAddress);

        return new MulticastDiscovery(clusterState, local, broadcastAddresses, sendSocket, receiveSocket);
    }

    private MulticastDiscovery(ClusterState clusterState, HostAndPort localSocketAddress,
            ArrayList<InetAddress> broadcastAddresses, DatagramSocket sendSocket, DatagramSocket receiveSocket)
            throws SocketException {
        super(clusterState, localSocketAddress, broadcastAddresses, sendSocket, receiveSocket);
    }

}
