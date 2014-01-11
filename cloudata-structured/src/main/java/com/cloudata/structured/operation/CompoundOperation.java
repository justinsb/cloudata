package com.cloudata.structured.operation;

import com.cloudata.btree.Btree;
import com.cloudata.btree.Transaction;
import com.cloudata.btree.operation.ComplexOperation;
import com.cloudata.structured.StructuredProtocol.StructuredAction;
import com.cloudata.structured.StructuredProtocol.StructuredActionResponse;
import com.cloudata.structured.StructuredProtocol.StructuredActionType;
import com.cloudata.structured.StructuredStore;
import com.google.common.base.Preconditions;

public class CompoundOperation extends StructuredOperationBase implements ComplexOperation<StructuredActionResponse> {

    private final StructuredStore store;

    public CompoundOperation(StructuredAction action, StructuredStore store) {
        super(action);
        this.store = store;
        Preconditions.checkState(action.getAction() == StructuredActionType.STRUCTURED_COMPOUND);
    }

    @Override
    public void doAction(Btree btree, Transaction txn) {
        for (StructuredAction child : action.getChildrenList()) {
            if (child.getStoreId() != action.getStoreId()) {
                throw new IllegalArgumentException();
            }
            StructuredOperation op = StructuredOperations.build(store, child);
            txn.doAction(btree, op);
            response.addChildren(op.getResult());
        }
    }

    @Override
    public boolean isReadOnly() {
        return StructuredOperations.isReadOnly(action);
    }

}
