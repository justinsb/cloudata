package com.cloudata.keyvalue.protobuf;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.KeyValueProtocol;
import com.cloudata.keyvalue.KeyValueProtocol.ActionResponse;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueRequest;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueResponse;
import com.cloudata.keyvalue.KeyValueStateMachine;
import com.cloudata.keyvalue.operation.KeyValueOperation;
import com.cloudata.keyvalue.operation.KeyValueOperations;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class KeyValueProtobufEndpoint implements KeyValueProtocol.KeyValueService.Interface {

    private static final Logger log = LoggerFactory.getLogger(KeyValueProtobufEndpoint.class);

    @Inject
    KeyValueStateMachine stateMachine;

    @Inject
    ListeningExecutorService executor;

    @Override
    public void execute(RpcController controller, KeyValueRequest request, final RpcCallback<KeyValueResponse> done) {
        try {
            KeyValueOperation operation = KeyValueOperations.build(request.getAction());

            ListenableFuture<ActionResponse> future = stateMachine.doActionAsync(operation);

            chain(controller, done, future);
        } catch (Exception e) {
            log.warn("Error processing protobuf request", e);
            controller.setFailed(e.getClass().getName());
            done.run(null);
        }
    }

    private void chain(final RpcController controller, final RpcCallback<KeyValueResponse> done,
            ListenableFuture<ActionResponse> future) {
        Futures.addCallback(future, new FutureCallback<ActionResponse>() {

            @Override
            public void onSuccess(ActionResponse actionResponse) {
                KeyValueResponse.Builder response = KeyValueResponse.newBuilder();
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
