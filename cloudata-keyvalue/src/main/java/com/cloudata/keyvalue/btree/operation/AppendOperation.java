package com.cloudata.keyvalue.btree.operation;

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
    public Value doAction(Value oldValue) {
        if (oldValue == null) {
            this.newLength = appendValue.size();
            return Value.fromRawBytes(appendValue);
        } else {
            Value appended = oldValue.concat(appendValue);

            this.newLength = appended.sizeAsBytes();
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
