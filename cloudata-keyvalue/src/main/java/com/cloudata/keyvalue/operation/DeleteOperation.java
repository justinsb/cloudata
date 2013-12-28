package com.cloudata.keyvalue.operation;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;
import com.google.protobuf.ByteString;

public class DeleteOperation extends com.cloudata.btree.operation.DeleteOperation implements KeyOperation<Integer> {

    public DeleteOperation(ByteString key) {
        super(key);
    }

    @Override
    public KvEntry.Builder serialize() {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setKey(key);
        b.setAction(KvAction.DELETE);
        return b;
    }
}
