package com.cloudata.keyvalue.operation;

import com.cloudata.btree.operation.BtreeOperation;
import com.cloudata.keyvalue.KeyValueProtocol.ActionResponse;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueAction;

public interface KeyValueOperation extends BtreeOperation<ActionResponse> {
    public abstract KeyValueAction serialize();

    public abstract long getStoreId();
}
