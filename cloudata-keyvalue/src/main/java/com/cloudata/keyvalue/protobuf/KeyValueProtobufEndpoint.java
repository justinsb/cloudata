package com.cloudata.keyvalue.protobuf;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.robotninjas.barge.RaftException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.BtreeQuery.KeyValueResultset;
import com.cloudata.btree.EntryListener;
import com.cloudata.btree.Keyspace;
import com.cloudata.keyvalue.KeyValueLog.KvAction;
import com.cloudata.keyvalue.KeyValueLog.KvEntry;
import com.cloudata.keyvalue.KeyValueProtocol;
import com.cloudata.keyvalue.KeyValueProtocol.Entry.Builder;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueRequest;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueResponse;
import com.cloudata.keyvalue.KeyValueStateMachine;
import com.cloudata.keyvalue.operation.DeleteOperation;
import com.cloudata.keyvalue.operation.SetOperation;
import com.cloudata.values.Value;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.ByteString;
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
            final ListenableFuture<KeyValueResponse> future;
            switch (request.getAction()) {
            case GET: {
                future = get(request);
                break;
            }

            case DELETE: {
                future = delete(request);
                break;
            }

            case SET: {
                future = set(request);
                break;
            }

            case LIST_ENTRIES_WITH_PREFIX: {
                future = scan(request, true);
                break;
            }

            case LIST_KEYS_WITH_PREFIX: {
                future = scan(request, false);
                break;
            }

            default:
                throw new IllegalArgumentException();
            }

            chain(controller, done, future);
        } catch (Exception e) {
            log.warn("Error processing protobuf request", e);
            controller.setFailed(e.getClass().getName());
            done.run(null);
        }
    }

    private ListenableFuture<KeyValueResponse> get(final KeyValueRequest request) {
        return executor.submit(new Callable<KeyValueResponse>() {
            @Override
            public KeyValueResponse call() throws Exception {
                long storeId = request.getStoreId();

                Keyspace keyspace = Keyspace.user(request.getSpaceId());

                ByteString key = request.getKey();

                Value v = stateMachine.get(storeId, keyspace, key);

                KeyValueResponse.Builder b = KeyValueResponse.newBuilder();
                b.setSuccess(true);
                if (v != null) {
                    b.setValue(ByteString.copyFrom(v.asBytes()));
                }
                return b.build();
            }
        });
    }

    private ListenableFuture<KeyValueResponse> delete(KeyValueRequest request) throws RaftException {

        Preconditions.checkArgument(!request.getIfNotExists());
        Preconditions.checkArgument(!request.hasValue());

        KvEntry entry = buildEntry(request, KvAction.DELETE);

        DeleteOperation operation = new DeleteOperation(entry);

        ListenableFuture<Integer> future = stateMachine.doActionAsync(operation);

        return Futures.transform(future, new Function<Integer, KeyValueResponse>() {
            @Override
            public KeyValueResponse apply(Integer input) {
                KeyValueResponse.Builder response = KeyValueResponse.newBuilder();
                if (input.intValue() == 1) {
                    response.setSuccess(true);
                } else {
                    response.setSuccess(false);
                }
                return response.build();
            }
        });
    }

    private KvEntry buildEntry(KeyValueRequest request, KvAction action) {
        Keyspace keyspace = Keyspace.user(request.getSpaceId());

        ByteString key = request.getKey();

        ByteString qualifiedKey = keyspace.mapToKey(key);

        KvEntry.Builder entry = KvEntry.newBuilder();
        entry.setStoreId(request.getStoreId());
        entry.setKey(qualifiedKey);
        if (request.getIfNotExists()) {
            Preconditions.checkArgument(!request.hasIfValue());
            entry.setIfNotExists(request.getIfNotExists());
        }
        if (request.hasIfValue()) {
            Preconditions.checkArgument(!request.getIfNotExists());
            entry.setIfValue(request.getIfValue());
        }
        if (request.hasValue()) {
            entry.setValue(ByteString.copyFrom(Value.fromRawBytes(request.getValue()).serialize()));
        }
        entry.setAction(action);
        return entry.build();
    }

    private ListenableFuture<KeyValueResponse> set(KeyValueRequest request) throws RaftException {
        KvEntry entry = buildEntry(request, KvAction.SET);

        SetOperation operation = new SetOperation(entry);

        ListenableFuture<Integer> future = stateMachine.doActionAsync(operation);

        return Futures.transform(future, new Function<Integer, KeyValueResponse>() {
            @Override
            public KeyValueResponse apply(Integer input) {
                KeyValueResponse.Builder response = KeyValueResponse.newBuilder();
                if (input.intValue() == 1) {
                    response.setSuccess(true);
                } else {
                    response.setSuccess(false);
                }
                return response.build();
            }
        });
    }

    private ListenableFuture<KeyValueResponse> scan(final KeyValueRequest request, final boolean returnEntries) {
        ListenableFuture<KeyValueResponse> future = executor.submit(new Callable<KeyValueResponse>() {

            @Override
            public KeyValueResponse call() throws Exception {
                long storeId = request.getStoreId();

                Keyspace keyspace = Keyspace.user(request.getSpaceId());
                ByteString key = request.getKey();

                BtreeQuery query = stateMachine.scan(storeId, keyspace, key);

                final KeyValueResponse.Builder b = KeyValueResponse.newBuilder();

                try (KeyValueResultset rs = query.execute()) {
                    rs.walk(new EntryListener() {
                        @Override
                        public boolean found(ByteBuffer key, Value value) {
                            if (returnEntries) {
                                Builder eb = b.addEntryBuilder();
                                eb.setKey(ByteString.copyFrom(key));
                                eb.setValue(ByteString.copyFrom(value.asBytes()));
                            } else {
                                b.addKey(ByteString.copyFrom(key));
                            }
                            return true;
                        }
                    });
                }
                return b.build();
            }
        });

        return future;
    }

    private void chain(final RpcController controller, final RpcCallback<KeyValueResponse> done,
            ListenableFuture<KeyValueResponse> future) {
        Futures.addCallback(future, new FutureCallback<KeyValueResponse>() {

            @Override
            public void onSuccess(KeyValueResponse result) {
                done.run(result);
            }

            @Override
            public void onFailure(Throwable t) {
                String reason = t.getClass().getName();
                controller.setFailed(reason);
                done.run(null);
            }

        });

    }
}
