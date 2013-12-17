package com.cloudata.btree.operation;

import com.cloudata.values.Value;

/**
 * An operation that operates on a single btree-row
 */
public interface RowOperation<V> extends BtreeOperation<V> {
    public Value doAction(Value oldValue);
}
