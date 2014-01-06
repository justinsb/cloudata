package com.cloudata.btree.operation;

import com.cloudata.btree.Btree;
import com.cloudata.btree.Transaction;

public interface ComplexOperation<V> extends BtreeOperation<V> {
    void doAction(Btree btree, Transaction txn);
}
