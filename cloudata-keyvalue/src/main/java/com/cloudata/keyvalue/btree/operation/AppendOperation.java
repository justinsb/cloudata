package com.cloudata.keyvalue.btree.operation;

import java.nio.ByteBuffer;

public class AppendOperation extends KeyOperation {

    final ByteBuffer appendValue;
    private int newLength;

    public AppendOperation(ByteBuffer appendValue) {
        this.appendValue = appendValue;
    }

    @Override
    public ByteBuffer doAction(ByteBuffer oldValue) {
        if (oldValue == null) {
            this.newLength = appendValue.remaining();
            return appendValue;
        } else {
            int n = oldValue.remaining() + appendValue.remaining();
            ByteBuffer appended = ByteBuffer.allocate(n);
            appended.put(oldValue.duplicate());
            appended.put(appendValue.duplicate());
            appended.flip();
            this.newLength = n;
            return appended;
        }
    }

    public int getNewLength() {
        return newLength;
    }

}
