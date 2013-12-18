package com.cloudata.clients;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Throwables;
import com.sun.jersey.api.client.ClientResponse;

public abstract class StreamingRecordsetBase<V> implements AutoCloseable, Iterable<V> {
    final ClientResponse response;

    boolean read;

    public StreamingRecordsetBase(ClientResponse response) {
        this.response = response;
    }

    @Override
    public void close() {
        response.close();
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
