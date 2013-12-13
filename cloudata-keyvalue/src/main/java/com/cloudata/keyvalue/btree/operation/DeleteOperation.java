package com.cloudata.keyvalue.btree.operation;

import java.nio.ByteBuffer;

public class DeleteOperation extends KeyOperation {

    @Override
    public ByteBuffer doAction(ByteBuffer oldValue) {
        return null; // Delete the value
    }

}
