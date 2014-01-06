package com.cloudata.keyvalue.operation;

import com.cloudata.btree.Btree;
import com.cloudata.btree.Transaction;
import com.cloudata.btree.operation.ComplexOperation;
import com.cloudata.keyvalue.KeyValueProtocol.ActionResponse;
import com.cloudata.keyvalue.KeyValueProtocol.ActionType;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueAction;
import com.google.common.base.Preconditions;

public class CompoundOperation implements KeyValueOperation, ComplexOperation<ActionResponse> {

    private final KeyValueAction entry;

    final ActionResponse.Builder response = ActionResponse.newBuilder();

    public CompoundOperation(KeyValueAction entry) {
        Preconditions.checkState(entry.getAction() == ActionType.COMPOUND);
        Preconditions.checkArgument(entry.hasStoreId());

        this.entry = entry;
    }

    @Override
    public KeyValueAction serialize() {
        return entry;
    }

    @Override
    public ActionResponse getResult() {
        return response.build();
    }

    @Override
    public void doAction(Btree btree, Transaction txn) {
        for (KeyValueAction child : entry.getChildrenList()) {
            if (child.getStoreId() != entry.getStoreId()) {
                throw new IllegalArgumentException();
            }
            KeyValueOperation op = KeyValueOperations.build(child);
            txn.doAction(btree, op);
            response.addChildren(op.getResult());
        }
    }

    @Override
    public boolean isReadOnly() {
        return KeyValueOperations.isReadOnly(entry);
    }

    @Override
    public long getStoreId() {
        return entry.getStoreId();
    }

}
