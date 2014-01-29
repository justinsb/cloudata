package com.cloudata.cluster;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;

public class BroadcastDiscovery extends CastingDiscovery {

    private static final Logger log = LoggerFactory.getLogger(BroadcastDiscovery.class);

    static final InetAddress BROADCAST_ADDRESS = InetAddresses.forString("255.255.255.255");

    public static BroadcastDiscovery build(ClusterState clusterState, HostAndPort local) throws SocketException {
        DatagramSocket sendSocket = new DatagramSocket();
        sendSocket.setBroadcast(true);

        InetAddress localAddress = InetAddresses.forString(local.getHostText());
        InetSocketAddress localSocketAddress = new InetSocketAddress(localAddress, local.getPort());
        DatagramSocket receiveSocket = new DatagramSocket(localSocketAddress);
        receiveSocket.setSoTimeout(1000);

        // Broadcast the message over all the network interfaces
        ArrayList<InetAddress> broadcastAddresses = Lists.newArrayList();
        broadcastAddresses.add(BROADCAST_ADDRESS);
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            if (networkInterface.isLoopback() || !networkInterface.isUp()) {
                continue; // Don't want to broadcast to the loopback interface
            }
            for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                InetAddress broadcast = interfaceAddress.getBroadcast();
                if (broadcast == null) {
                    continue;
                }
                broadcastAddresses.add(broadcast);

                log.info("Found broadcast: {}", broadcast);
            }
        }

        return new BroadcastDiscovery(clusterState, local, broadcastAddresses, sendSocket, receiveSocket);
    }

    private BroadcastDiscovery(ClusterState clusterState, HostAndPort localSocketAddress,
            ArrayList<InetAddress> broadcastAddresses, DatagramSocket sendSocket, DatagramSocket receiveSocket)
            throws SocketException {
        super(clusterState, localSocketAddress, broadcastAddresses, sendSocket, receiveSocket);
    }

}
