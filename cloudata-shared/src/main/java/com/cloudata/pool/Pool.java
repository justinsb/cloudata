package com.cloudata.pool;

public interface Pool<T extends AutoCloseable> {
    void release(T t, boolean reuse);

    Pooled<T> borrow();
}
