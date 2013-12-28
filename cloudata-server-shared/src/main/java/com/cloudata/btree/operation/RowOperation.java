package com.cloudata.btree.operation;

import java.nio.ByteBuffer;

import com.cloudata.values.Value;

/**
 * An operation that operates on a single btree-row
 */
public interface RowOperation<V> extends BtreeOperation<V> {
    public Value doAction(Value oldValue);

    /**
     * Gets the (fully-qualified) key
     */
    public ByteBuffer getKey();
}
