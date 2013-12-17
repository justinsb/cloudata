package com.cloudata.keyvalue.operation;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;

public class DeleteOperation extends com.cloudata.btree.operation.DeleteOperation implements KeyOperation<Integer> {
    @Override
    public KvEntry.Builder serialize() {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setAction(KvAction.DELETE);
        return b;
    }
}
