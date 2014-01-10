package com.cloudata.clients;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Throwables;

public abstract class StreamingRecordsetBase<V> implements AutoCloseable, Iterable<V> {
    final Closeable httpResponse;

    boolean read;

    public StreamingRecordsetBase(Closeable httpResponse) {
        this.httpResponse = httpResponse;
    }

    @Override
    public void close() throws IOException {
        httpResponse.close();
    }

    @Override
    public Iterator<V> iterator() {
        if (read) {
            throw new IllegalStateException();
        }
        read = true;

        return new Iterator<V>() {
            V next;
            boolean done;

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public V next() {
                ensureHaveNext();

                if (next != null) {
                    V ret = next;
                    next = null;
                    return ret;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public boolean hasNext() {
                ensureHaveNext();

                return next != null;
            }

            private void ensureHaveNext() {
                if (next == null) {
                    if (!done) {
                        try {
                            next = read();
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                        if (next == null) {
                            done = true;
                        }
                    }
                }
            }

        };

    }

    protected abstract V read() throws IOException;
}
