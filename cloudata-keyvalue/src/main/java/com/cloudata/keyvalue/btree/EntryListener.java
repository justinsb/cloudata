package com.cloudata.keyvalue.btree;

import java.nio.ByteBuffer;

public interface EntryListener {

    public boolean found(ByteBuffer key, ByteBuffer value);
}
