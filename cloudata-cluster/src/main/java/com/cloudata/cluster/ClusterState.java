package com.cloudata.cluster;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.cluster.ClusterProtocol.BroadcastMessage;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

public class ClusterState {

    private static final Logger log = LoggerFactory.getLogger(ClusterState.class);

    final ConcurrentMap<ClusterId, ClusterPeer> peers = Maps.newConcurrentMap();

    public ClusterPeer getPeer(ClusterId peerId, InetSocketAddress remote) {
        ClusterPeer peer = peers.get(peerId);
        if (peer == null) {
            HostAndPort remoteHostAndPort = HostAndPort.fromParts(remote.getHostString(), remote.getPort());
            peer = new ClusterPeer(peerId, remoteHostAndPort);
            ClusterPeer existing = peers.putIfAbsent(peerId, peer);
            if (existing != null) {
                return existing;
            }
        }

        peer.ensureStarted();

        return peer;
    }

    public void notifyBroadcast(InetAddress src, BroadcastMessage message) {
        log.info("Got message: {} {}", src, message);
        ClusterId clusterId = new ClusterId(message.getId());

        ClusterPeer peer = peers.get(clusterId);
        if (peer == null) {
            InetAddress address;
            try {
                address = InetAddress.getByAddress(message.getAddress().toByteArray());
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Invalid address in broadcast", e);
            }
            InetSocketAddress remote = new InetSocketAddress(address, message.getPort());
            peer = getPeer(clusterId, remote);
            peer.ensureStarted();
        }
        peer.notifyBroadcast(src, message);
    }

    public List<ClusterPeer> choosePeers(List<ClusterId> aliveIds, List<ClusterId> deadIds, int pickCount) {
        Set<ClusterId> aliveInCluster = Sets.newHashSet(aliveIds);
        Set<ClusterId> deadInCluster = Sets.newHashSet(deadIds);

        // TODO: Make this _much_ more sensible
        log.warn("Peer choosing is primitive");

        List<ClusterId> candidates = Lists.newArrayList();

        for (ClusterPeer peer : peers.values()) {
            ClusterId peerId = peer.getId();

            if (aliveInCluster.contains(peerId)) {
                continue;
            }

            if (deadInCluster.contains(peerId)) {
                continue;
            }

            if (!peer.isHealthy()) {
                log.debug("Ignoring unhealthy peer: {}", peer);
                continue;
            }
            candidates.add(peerId);
        }

        List<ClusterPeer> chosen = Lists.newArrayList();
        while (chosen.size() < pickCount) {
            if (candidates.isEmpty()) {
                break;
            }
            ClusterId head = candidates.remove(0);
            ClusterPeer peer = peers.get(head);
            if (peer == null) {
                // Concurrent change?
                throw new IllegalStateException();
            }
            chosen.add(peer);
        }

        return chosen;
    }

}
