package com.cloudata.cluster;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import com.cloudata.clients.protobuf.ProtobufRpcClient;
import com.cloudata.cluster.ClusterProtocol.GossipRequest;
import com.cloudata.cluster.ClusterProtocol.GossipResponse;
import com.cloudata.structured.StructuredProtocol.StructuredRpcService;
import com.google.common.util.concurrent.ListenableFuture;

public class ClusterClient {
    final ProtobufRpcClient protobufRpcClient;

    public ClusterClient(ProtobufRpcClient protobufRpcClient) {
        this.protobufRpcClient = protobufRpcClient;
    }

    static final ProtobufRpcClient.RpcMethod<GossipRequest, GossipResponse> METHOD_GOSSIP = ProtobufRpcClient.RpcMethod
            .create(StructuredRpcService.getDescriptor(), "Gossip", GossipRequest.getDefaultInstance(),
                    GossipResponse.getDefaultInstance());

    @Nonnull
    public ListenableFuture<GossipResponse> gossip(@Nonnull GossipRequest request) {
        checkNotNull(request);
        return protobufRpcClient.call(METHOD_GOSSIP, request);
    }

}
