package com.cloudata.keyvalue.btree.operation;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;
import com.google.protobuf.ByteString;

public class AppendOperation extends KeyOperation<Integer> {

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

    @Override
    public KvEntry.Builder serialize() {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setAction(KvAction.APPEND);
        b.setValue(ByteString.copyFrom(appendValue));
        return b;
    }

    @Override
    public Integer getResult() {
        return newLength;
    }

}
