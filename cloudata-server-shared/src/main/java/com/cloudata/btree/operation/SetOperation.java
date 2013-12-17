package com.cloudata.btree.operation;

import com.cloudata.values.Value;

public class SetOperation implements RowOperation<Void> {

    protected final Value newValue;

    public SetOperation(Value newValue) {
        this.newValue = newValue;
    }

    @Override
    public Value doAction(Value oldValue) {
        return newValue;
    }

    @Override
    public Void getResult() {
        return null;
    }
}
