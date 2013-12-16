package com.cloudata.keyvalue.btree.operation;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;
import com.google.protobuf.ByteString;

public class SetOperation extends KeyOperation<Void> {

    final Value newValue;

    public SetOperation(Value newValue) {
        this.newValue = newValue;
    }

    @Override
    public Value doAction(Value oldValue) {
        return newValue;
    }

    @Override
    public KvEntry.Builder serialize() {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setAction(KvAction.SET);
        b.setValue(ByteString.copyFrom(newValue.serialize()));
        return b;
    }

    @Override
    public Void getResult() {
        return null;
    }
}
