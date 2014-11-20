//package com.cloudata.cluster;
//
//import java.io.ByteArrayInputStream;
//import java.io.IOException;
//import java.net.DatagramPacket;
//import java.net.DatagramSocket;
//import java.net.InetAddress;
//import java.net.SocketException;
//import java.net.SocketTimeoutException;
//import java.util.ArrayList;
//import java.util.concurrent.TimeUnit;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.cloudata.TimeSpan;
//import com.cloudata.cluster.ClusterProtocol.BroadcastMessage;
//import com.cloudata.services.ThreadService;
//import com.google.common.net.HostAndPort;
//
//public abstract class CastingDiscovery extends ThreadService implements Discovery {
//
//    private static final Logger log = LoggerFactory.getLogger(CastingDiscovery.class);
//
//    final HostAndPort localSocketAddress;
//    final ArrayList<InetAddress> broadcastAddresses;
//    final DatagramSocket sendSocket;
//    final DatagramSocket receiveSocket;
//
//    BroadcastMessage message;
//
//    final int port;
//
//    private final ClusterState clusterState;
//
//    protected CastingDiscovery(ClusterState clusterState, HostAndPort localSocketAddress,
//            ArrayList<InetAddress> broadcastAddresses, DatagramSocket sendSocket, DatagramSocket receiveSocket)
//            throws SocketException {
//        this.clusterState = clusterState;
//        this.localSocketAddress = localSocketAddress;
//        this.broadcastAddresses = broadcastAddresses;
//        this.sendSocket = sendSocket;
//        this.receiveSocket = receiveSocket;
//
//        this.port = localSocketAddress.getPort();
//    }
//
//    @Override
//    protected void run0() {
//        while (!shouldStop()) {
//            try {
//                doCycle();
//            } catch (Throwable t) {
//                log.error("Unexpected error during peer discovery", t);
//                TimeSpan.seconds(5).sleep();
//            }
//        }
//    }
//
//    private void doCycle() {
//        byte[] buffer = new byte[12000];
//        byte[] data = message.toByteArray();
//        for (InetAddress broadcastAddress : broadcastAddresses) {
//            DatagramPacket p = new DatagramPacket(data, data.length, broadcastAddress, port);
//            try {
//                sendSocket.send(p);
//                log.debug("Sent broadcast: to {} {}", broadcastAddress, message);
//            } catch (IOException e) {
//                log.warn("Error sending broadcast", e);
//            }
//        }
//
//        long nextBroadcast = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
//
//        do {
//            DatagramPacket packet = new DatagramPacket(buffer, 0, buffer.length);
//            boolean receieved = false;
//            try {
//                receiveSocket.receive(packet);
//                receieved = true;
//            } catch (SocketTimeoutException e) {
//                // Ignore
//            } catch (IOException e) {
//                log.warn("Error reading from socket", e);
//            }
//            if (receieved) {
//                BroadcastMessage message = null;
//
//                if (packet.getLength() == buffer.length) {
//                    log.warn("Message likely truncated; won't try to decode");
//                } else {
//                    try {
//                        message = BroadcastMessage.parseFrom(new ByteArrayInputStream(buffer, 0, packet.getLength()));
//                    } catch (IOException e) {
//                        log.warn("Error parsing broadcast message", e);
//                    }
//                }
//
//                gotMessage(packet.getAddress(), message);
//            }
//        } while (!shouldStop() && System.nanoTime() < nextBroadcast);
//    }
//
//    private void gotMessage(InetAddress src, BroadcastMessage message) {
//        clusterState.notifyBroadcast(src, message);
//    }
//}
