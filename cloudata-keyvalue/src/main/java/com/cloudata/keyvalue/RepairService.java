package com.cloudata.keyvalue;

import java.util.List;

import javax.inject.Inject;

import org.robotninjas.barge.NoLeaderException;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.RaftClusterHealth;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.Replica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.TimeSpan;
import com.cloudata.cluster.ClusterId;
import com.cloudata.cluster.ClusterPeer;
import com.cloudata.cluster.ClusterService;
import com.cloudata.services.ThreadService;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class RepairService extends ThreadService {

    private static final Logger log = LoggerFactory.getLogger(RepairService.class);

    private final RaftService raft;
    private final ClusterService cluster;

    @Inject
    protected RepairService(RaftService raft, ClusterService cluster) {
        this.raft = raft;
        this.cluster = cluster;
    }

    private void doRepairCycle() {
        if (!raft.isLeader()) {
            return;
        }

        RaftClusterHealth clusterHealth = null;
        try {
            clusterHealth = raft.getClusterHealth();
        } catch (NotLeaderException e) {
            return;
        } catch (NoLeaderException e) {
            return;
        } catch (RaftException e) {
            log.warn("Error while checking cluster health", e);
            return;
        }

        if (clusterHealth == null) {
            // This happens if we're in transition
            log.debug("Unable to fetch cluster health (in transition?)");
            return;
        }

        RaftMembership oldMembership = clusterHealth.getClusterMembership();
        List<Replica> deadPeers = clusterHealth.getDeadPeers();
        if (deadPeers.isEmpty()) {
            return;
        }

        log.warn("Detected dead peers: {}", deadPeers);

        List<String> newMembers = Lists.newArrayList();

        List<ClusterId> aliveMembers = Lists.newArrayList();
        List<ClusterId> deadMembers = Lists.newArrayList();

        for (String oldMember : oldMembership.getMembers()) {
            ClusterId clusterId = new ClusterId(oldMember);
            if (deadPeers.contains(oldMember)) {
                deadMembers.add(clusterId);
            } else {
                aliveMembers.add(clusterId);

                newMembers.add(oldMember);
            }
        }

        List<ClusterPeer> newPeers = cluster.choosePeers(aliveMembers, deadMembers, deadMembers.size());

        log.warn("Adding peers: {}", newPeers);

        for (ClusterPeer newPeer : newPeers) {
            HostAndPort endpoint = newPeer.getEndpoint();
            Replica replica = Replica.fromString(endpoint.toString());
            newMembers.add(replica.getKey());
        }

        RaftMembership newMembership = new RaftMembership(-1, newMembers);

        log.warn("Changing configuration from {} to {}", oldMembership, newMembership);

        ListenableFuture<Boolean> future = raft.setConfiguration(oldMembership, newMembership);
        try {
            Boolean result = Futures.getUnchecked(future);
            if (!Boolean.TRUE.equals(result)) {
                log.warn("Unexpectedly failed to set new configuration");
            }
        } catch (Exception e) {
            log.warn("Error reconfiguring cluster", e);
        }
    }

    @Override
    protected void run0() {
        while (!shouldStop()) {
            try {
                doRepairCycle();
            } catch (Throwable t) {
                log.error("Unexpected error during repair cycle", t);
            }

            TimeSpan.seconds(15).sleep();
        }
    }

}
