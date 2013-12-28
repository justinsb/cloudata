package com.cloudata.btree.operation;

import com.cloudata.btree.Btree;
import com.cloudata.btree.WriteTransaction;

public interface ComplexOperation<V> extends BtreeOperation<V> {

    void doAction(Btree btree, WriteTransaction writeTransaction);
}
