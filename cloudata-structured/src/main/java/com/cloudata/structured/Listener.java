package com.cloudata.structured;

public interface Listener<V> {
    public boolean next(V value);

    public void done();
}
