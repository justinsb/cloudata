package com.cloudata.keyvalue.btree.operation;

import java.nio.ByteBuffer;

public abstract class KeyOperation {
    public abstract ByteBuffer doAction(ByteBuffer oldValue);
}
