package com.cloudata.btree;

import java.nio.ByteBuffer;

import com.cloudata.values.Value;

public interface EntryListener {

    /*
     * Called for each entry.
     * 
     * Return false to terminate the walk.
     */
    public boolean found(ByteBuffer key, Value value);
}
