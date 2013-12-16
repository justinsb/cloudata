package com.cloudata.keyvalue.btree.operation;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;
import com.google.protobuf.ByteString;

public class AppendOperation extends KeyOperation<Integer> {

    final ByteString appendValue;
    private int newLength;

    public AppendOperation(ByteString appendValue) {
        this.appendValue = appendValue;
    }

    @Override
    public ByteBuffer doAction(ByteBuffer oldValue) {
        if (oldValue == null) {
            this.newLength = appendValue.size();
            return Values.fromRawBytes(appendValue);
        } else {
            ByteBuffer appended = Values.concat(oldValue, appendValue);

            this.newLength = Values.sizeAsBytes(appended);
            return appended;
        }
    }

    @Override
    public KvEntry.Builder serialize() {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setAction(KvAction.APPEND);
        b.setValue(appendValue);
        return b;
    }

    @Override
    public Integer getResult() {
        return newLength;
    }

}
