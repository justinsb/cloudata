package com.cloudata.keyvalue.operation;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;
import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class SetOperation extends com.cloudata.btree.operation.SetOperation implements KeyOperation<Void> {

    public SetOperation(ByteString key, Value newValue) {
        super(key, newValue);
    }

    @Override
    public KvEntry.Builder serialize() {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setAction(KvAction.SET);
        b.setKey(key);
        b.setValue(ByteString.copyFrom(newValue.serialize()));
        return b;
    }

}
