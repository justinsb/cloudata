package com.cloudata.cluster;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.cluster.ClusterProtocol.BroadcastMessage;
import com.cloudata.cluster.ClusterProtocol.GossipRequest;
import com.cloudata.cluster.ClusterProtocol.GossipResponse;
import com.google.common.net.HostAndPort;

public class ClusterPeer {

    private static final Logger log = LoggerFactory.getLogger(ClusterPeer.class);

    final ClusterId id;
    final HostAndPort endpoint;

    public ClusterPeer(ClusterId id, HostAndPort endpoint) {
        this.id = id;
        this.endpoint = endpoint;
    }

    long lastMessage;

    private void updateTimeouts() {
        lastMessage = System.nanoTime();
    }

    public ClusterId getId() {
        return id;
    }

    public HostAndPort getEndpoint() {
        return endpoint;
    }

    public GossipResponse gossip(GossipRequest request) {
        updateTimeouts();
        return GossipResponse.newBuilder().build();
    }

    public boolean isHealthy() {
        long now = System.nanoTime();
        boolean timedOut = (now < (lastMessage + TimeUnit.SECONDS.toNanos(30)));

        if (timedOut) {
            log.debug("Node not healthy: timed out: {}", this);
            return false;
        }

        return true;
    }

    public void notifyBroadcast(InetAddress src, BroadcastMessage message) {
        updateTimeouts();
    }

    public void ensureStarted() {

    }
}
