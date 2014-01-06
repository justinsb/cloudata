package com.cloudata.clients.keyvalue.cloudata;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import com.cloudata.clients.protobuf.ProtobufRpcClient;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueRequest;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueResponse;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueService;
import com.google.common.util.concurrent.ListenableFuture;

public class CloudataKeyValueClient {
    final ProtobufRpcClient protobufRpcClient;

    public CloudataKeyValueClient(ProtobufRpcClient protobufRpcClient) {
        this.protobufRpcClient = protobufRpcClient;
    }

    static final ProtobufRpcClient.RpcMethod<KeyValueRequest, KeyValueResponse> METHOD_EXECUTE = ProtobufRpcClient.RpcMethod
            .create(KeyValueService.getDescriptor(), "Execute", KeyValueResponse.class,
                    KeyValueResponse.getDefaultInstance());

    @Nonnull
    public ListenableFuture<KeyValueResponse> execute(@Nonnull KeyValueRequest request) {
        checkNotNull(request);
        return protobufRpcClient.call(METHOD_EXECUTE, request);
    }

}
