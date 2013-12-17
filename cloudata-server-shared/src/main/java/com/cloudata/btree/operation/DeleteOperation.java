package com.cloudata.btree.operation;

import com.cloudata.values.Value;

public class DeleteOperation implements RowOperation<Integer> {

    private int deleteCount;

    @Override
    public Value doAction(Value oldValue) {
        if (oldValue != null) {
            deleteCount++;
        }
        return null; // Delete the value
    }

    @Override
    public Integer getResult() {
        return deleteCount;
    }
}