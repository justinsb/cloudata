package com.cloudata.clients.structured.cloudata;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import com.cloudata.clients.protobuf.ProtobufRpcClient;
import com.cloudata.structured.StructuredProtocol.StructuredRequest;
import com.cloudata.structured.StructuredProtocol.StructuredResponse;
import com.cloudata.structured.StructuredProtocol.StructuredRpcService;
import com.google.common.util.concurrent.ListenableFuture;

public class CloudataStructuredClient {
    final ProtobufRpcClient protobufRpcClient;

    public CloudataStructuredClient(ProtobufRpcClient protobufRpcClient) {
        this.protobufRpcClient = protobufRpcClient;
    }

    static final ProtobufRpcClient.RpcMethod<StructuredRequest, StructuredResponse> METHOD_EXECUTE = ProtobufRpcClient.RpcMethod
            .create(StructuredRpcService.getDescriptor(), "Execute", StructuredRequest.getDefaultInstance(),
                    StructuredResponse.getDefaultInstance());

    @Nonnull
    public ListenableFuture<StructuredResponse> execute(@Nonnull StructuredRequest request) {
        checkNotNull(request);
        return protobufRpcClient.call(METHOD_EXECUTE, request);
    }

}
