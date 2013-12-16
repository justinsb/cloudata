package com.cloudata.keyvalue.btree.operation;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.KeyValueProto.KvEntry;

public class DeleteOperation extends KeyOperation<Integer> {

    private int deleteCount;

    @Override
    public Value doAction(Value oldValue) {
        if (oldValue != null) {
            deleteCount++;
        }
        return null; // Delete the value
    }

    @Override
    public KvEntry.Builder serialize() {
        KvEntry.Builder b = KvEntry.newBuilder();
        b.setAction(KvAction.DELETE);
        return b;
    }

    @Override
    public Integer getResult() {
        return deleteCount;
    }
}
