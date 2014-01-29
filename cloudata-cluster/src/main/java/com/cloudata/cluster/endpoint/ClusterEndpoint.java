package com.cloudata.cluster.endpoint;

import java.net.InetSocketAddress;

import org.robotninjas.protobuf.RpcContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.cluster.ClusterId;
import com.cloudata.cluster.ClusterPeer;
import com.cloudata.cluster.ClusterProtocol;
import com.cloudata.cluster.ClusterProtocol.GossipRequest;
import com.cloudata.cluster.ClusterProtocol.GossipResponse;
import com.cloudata.cluster.ClusterState;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class ClusterEndpoint implements ClusterProtocol.ClusterRpcService.Interface {

    private static final Logger log = LoggerFactory.getLogger(ClusterEndpoint.class);

    final ClusterState cluster;

    public ClusterEndpoint(ClusterState cluster) {
        this.cluster = cluster;
    }

    @Override
    public void gossip(RpcController controller, GossipRequest request, final RpcCallback<GossipResponse> done) {
        try {
            ClusterId peerId = new ClusterId(request.getFromId());

            RpcContext rpcContext = (RpcContext) controller;
            InetSocketAddress remote = (InetSocketAddress) rpcContext.getRemote();

            ClusterPeer peer = cluster.getPeer(peerId, remote);

            GossipResponse response = peer.gossip(request);

            done.run(response);

            // chain(controller, done, future);
        } catch (Exception e) {
            log.warn("Error processing protobuf request", e);
            controller.setFailed(e.getClass().getName());
            done.run(null);
        }
    }

    private static <T> void chain(final RpcController controller, final RpcCallback<T> done, ListenableFuture<T> future) {
        Futures.addCallback(future, new FutureCallback<T>() {

            @Override
            public void onSuccess(T t) {
                done.run(t);
            }

            @Override
            public void onFailure(Throwable t) {
                log.warn("Error running protobuf command", t);

                String reason = t.getClass().getName();
                controller.setFailed(reason);
                done.run(null);
            }

        });

    }
}
