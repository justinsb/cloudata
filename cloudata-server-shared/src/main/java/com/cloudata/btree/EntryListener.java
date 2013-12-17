package com.cloudata.btree;

import java.nio.ByteBuffer;

import com.cloudata.values.Value;

public interface EntryListener {

    public boolean found(ByteBuffer key, Value value);
}
