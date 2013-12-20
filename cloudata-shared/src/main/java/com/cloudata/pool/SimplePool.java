package com.cloudata.pool;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

public class SimplePool<T extends AutoCloseable> implements Pool<T> {

    private static final Logger log = LoggerFactory.getLogger(SimplePool.class);

    final ConcurrentLinkedQueue<T> pool;
    final Callable<T> factory;

    public SimplePool(Callable<T> factory) {
        this.factory = factory;
        this.pool = new ConcurrentLinkedQueue<T>();
    }

    @Override
    public void release(T t, boolean reuse) {
        boolean close = !reuse;
        if (reuse) {
            if (!pool.add(t)) {
                close = true;
            }
        }

        if (close) {
            try {
                t.close();
            } catch (Exception e) {
                log.warn("Error while closing poisoned pooled object", e);
            }
        }

    }

    @Override
    public Pooled<T> borrow() {
        T t = pool.poll();
        if (t == null) {
            try {
                t = factory.call();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        return new Pooled<T>(this, t);
    }

}