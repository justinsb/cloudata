package com.cloudata.btree.operation;

public interface BtreeOperation<V> {
    public abstract V getResult();

    public boolean isReadOnly();
}
