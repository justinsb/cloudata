package com.cloudata.keyvalue.btree.operation;

import java.nio.ByteBuffer;

public class SetOperation extends KeyOperation {

    final ByteBuffer newValue;

    public SetOperation(ByteBuffer newValue) {
        this.newValue = newValue;
    }

    @Override
    public ByteBuffer doAction(ByteBuffer oldValue) {
        return newValue;
    }

}
