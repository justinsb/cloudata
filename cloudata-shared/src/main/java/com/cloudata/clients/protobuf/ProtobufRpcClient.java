package com.cloudata.clients.protobuf;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.pool.ObjectPool;
import org.robotninjas.protobuf.netty.RpcException;
import org.robotninjas.protobuf.netty.client.ClientController;
import org.robotninjas.protobuf.netty.client.NettyRpcChannel;
import org.robotninjas.protobuf.netty.client.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

@Immutable
public class ProtobufRpcClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcClient.class);
    private static final long DEFAULT_TIMEOUT = 2000;

    private final ObjectPool<ListenableFuture<NettyRpcChannel>> channelPool;

    ProtobufRpcClient(@Nonnull ObjectPool<ListenableFuture<NettyRpcChannel>> channelPool) {
        this.channelPool = checkNotNull(channelPool);
    }

    public <RequestT extends Message, ResponseT extends Message> ListenableFuture<ResponseT> call(
            final RpcMethod<RequestT, ResponseT> call, final RequestT request) {
        ListenableFuture<NettyRpcChannel> channel = null;
        try {

            channel = channelPool.borrowObject();
            ListenableFuture<ResponseT> response = transform(channel, new AsyncFunction<NettyRpcChannel, ResponseT>() {
                @Override
                public ListenableFuture<ResponseT> apply(NettyRpcChannel channel) throws Exception {
                    ClientController controller = new ClientController(channel);
                    controller.setTimeout(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
                    RpcHandlerFuture<ResponseT> responseHandler = new RpcHandlerFuture<ResponseT>(controller);
                    call.call(channel, controller, request, responseHandler);
                    return responseHandler;
                }
            });

            response.addListener(returnChannel(channel), sameThreadExecutor());

            return response;

        } catch (Exception e) {

            try {
                channelPool.invalidateObject(channel);
            } catch (Exception e1) {
            }
            channel = null;
            return immediateFailedFuture(e);

        } finally {

            try {
                if (null != channel) {
                    channelPool.returnObject(channel);
                }
            } catch (Exception e) {
                // ignored
            }

        }
    }

    private Runnable returnChannel(final ListenableFuture<NettyRpcChannel> channel) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    channelPool.returnObject(channel);
                } catch (Exception e) {
                }
            }
        };
    }

    public static class RpcMethod<RequestT extends Message, ResponseT extends Message> {

        final Descriptors.MethodDescriptor method;
        final ResponseT responseDefaultInstance;

        public RpcMethod(MethodDescriptor method, ResponseT responseDefaultInstance) {
            this.method = method;
            this.responseDefaultInstance = responseDefaultInstance;
        }

        public void call(com.google.protobuf.RpcChannel channel, RpcController controller, RequestT request,
                RpcCallback<ResponseT> callback) {
            channel.callMethod(method, controller, request, responseDefaultInstance, (RpcCallback<Message>) callback);
        }

        public static <RequestT extends Message, ResponseT extends Message> RpcMethod<RequestT, ResponseT> create(
                ServiceDescriptor descriptor, String methodName, RequestT requestDefaultInstance,
                ResponseT responseDefaultInstance) {
            MethodDescriptor method = descriptor.findMethodByName(methodName);
            Preconditions.checkArgument(method != null);

            Preconditions.checkArgument(requestDefaultInstance.getDescriptorForType() == method.getInputType());
            Preconditions.checkArgument(responseDefaultInstance.getDescriptorForType() == method.getOutputType());

            return new RpcMethod<RequestT, ResponseT>(method, responseDefaultInstance);
        }
    }

    @Immutable
    private static class RpcHandlerFuture<T> extends AbstractFuture<T> implements RpcCallback<T> {

        @Nonnull
        private final ClientController controller;

        private RpcHandlerFuture(@Nonnull ClientController controller) {
            checkNotNull(controller);

            this.controller = controller;
        }

        @Override
        public void run(@Nullable T parameter) {

            if (isCancelled()) {
                return;
            }

            if (null == parameter) {
                setException(new RpcException(null, controller.errorText()));
            } else {
                set(parameter);
            }

        }

    }

}
