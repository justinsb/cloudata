package com.cloudata.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class Pooled<T extends AutoCloseable> implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Pooled.class);

    final Pool<T> pool;
    final T t;

    private State state = State.READY;

    enum State {
        READY, POISONED, CLOSED
    }

    public Pooled(Pool<T> pool, T t) {
        Preconditions.checkArgument(pool != null);
        Preconditions.checkArgument(t != null);

        this.pool = pool;
        this.t = t;

        state = State.READY;

    }

    public T get() {
        Preconditions.checkState(state == State.READY);
        return t;
    }

    public void poison() {
        Preconditions.checkState(state == State.READY);
        state = State.POISONED;
    }

    @Override
    public void close() {
        switch (state) {
        case READY:
            pool.release(t, true);
            state = State.CLOSED;
            break;
        case POISONED:
            pool.release(t, false);
            state = State.CLOSED;
            break;
        case CLOSED:
            throw new IllegalStateException();
        default:
            throw new IllegalStateException();
        }

    }
}
