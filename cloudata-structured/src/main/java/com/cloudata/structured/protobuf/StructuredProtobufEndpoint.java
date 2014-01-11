package com.cloudata.structured.protobuf;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.structured.StructuredProtocol.StructuredActionResponse;
import com.cloudata.structured.StructuredProtocol.StructuredRequest;
import com.cloudata.structured.StructuredProtocol.StructuredResponse;
import com.cloudata.structured.StructuredStateMachine;
import com.cloudata.structured.StructuredStore;
import com.cloudata.structured.operation.StructuredOperation;
import com.cloudata.structured.operation.StructuredOperations;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class StructuredProtobufEndpoint implements
        com.cloudata.structured.StructuredProtocol.StructuredRpcService.Interface {

    private static final Logger log = LoggerFactory.getLogger(StructuredProtobufEndpoint.class);

    @Inject
    StructuredStateMachine stateMachine;

    @Inject
    ListeningExecutorService executor;

    @Override
    public void execute(RpcController controller, StructuredRequest request, final RpcCallback<StructuredResponse> done) {
        try {
            // We don't need the store until we actually apply the operation
            StructuredStore store = null;
            StructuredOperation operation = StructuredOperations.build(store, request.getAction());

            ListenableFuture<StructuredActionResponse> future = stateMachine.doActionAsync(operation);

            chain(controller, done, future);
        } catch (Exception e) {
            log.warn("Error processing protobuf request", e);
            controller.setFailed(e.getClass().getName());
            done.run(null);
        }
    }

    private void chain(final RpcController controller, final RpcCallback<StructuredResponse> done,
            ListenableFuture<StructuredActionResponse> future) {
        Futures.addCallback(future, new FutureCallback<StructuredActionResponse>() {

            @Override
            public void onSuccess(StructuredActionResponse actionResponse) {
                StructuredResponse.Builder response = StructuredResponse.newBuilder();
                response.setActionResponse(actionResponse);
                done.run(response.build());
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
