package com.cloudata.btree;

import io.netty.util.ReferenceCounted;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class Cleanup {

    public static <V> void add(ListenableFuture<V> future, final ReferenceCounted counted) {
        Futures.addCallback(future, new FutureCallback<V>() {
            @Override
            public void onSuccess(V result) {
                counted.release();
            }

            @Override
            public void onFailure(Throwable t) {
                counted.release();
            }
        });
    }

}
