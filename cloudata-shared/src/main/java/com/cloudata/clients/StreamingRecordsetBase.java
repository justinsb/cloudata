package com.cloudata.clients;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
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
        InputStream is = response.getEntityInputStream();

        final DataInputStream dis = new DataInputStream(is);

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
                            next = read(dis);
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

    protected abstract V read(DataInputStream dis) throws IOException;
}
