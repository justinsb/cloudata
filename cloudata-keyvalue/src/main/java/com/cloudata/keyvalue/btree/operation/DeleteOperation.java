package com.cloudata.keyvalue.btree.operation;

import java.nio.ByteBuffer;

public class DeleteOperation extends KeyOperation {

    private int deleteCount;

    @Override
    public ByteBuffer doAction(ByteBuffer oldValue) {
        if (oldValue != null) {
            deleteCount++;
        }
        return null; // Delete the value
    }

    public int getDeleteCount() {
        return deleteCount;
    }

}
