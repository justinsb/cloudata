package com.cloudata.keyvalue.btree.operation;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;
import com.google.protobuf.ByteString;

public class SetOperation extends KeyOperation<Void> {

    final ByteBuffer newValue;

    public SetOperation(ByteBuffer newValue) {
        this.newValue = newValue;
    }

    @Override
    public ByteBuffer doAction(ByteBuffer oldValue) {
        return newValue;
    }

    @Override
    public KvEntry.Builder serialize() {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setAction(KvAction.SET);
        b.setValue(ByteString.copyFrom(newValue));
        return b;
    }

    @Override
    public Void getResult() {
        return null;
    }
}
