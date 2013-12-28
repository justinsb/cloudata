package com.cloudata.keyvalue.operation;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;
import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class AppendOperation implements KeyOperation<Integer> {

    final ByteString appendValue;
    private int newLength;
    private final ByteString key;

    public AppendOperation(ByteString key, ByteString appendValue) {
        this.key = key;
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
        b.setKey(key);
        b.setValue(appendValue);
        return b;
    }

    @Override
    public Integer getResult() {
        return newLength;
    }

    @Override
    public ByteBuffer getKey() {
        return key.asReadOnlyByteBuffer();
    }

}
