package com.cloudata.btree;

import com.cloudata.values.Value;

public interface BtreeOperation<V> {
    public Value doAction(Value oldValue);

    public abstract V getResult();
}
