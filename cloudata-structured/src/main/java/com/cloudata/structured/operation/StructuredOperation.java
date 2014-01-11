package com.cloudata.structured.operation;

import com.cloudata.btree.operation.BtreeOperation;
import com.cloudata.structured.StructuredProtocol.StructuredAction;
import com.cloudata.structured.StructuredProtocol.StructuredActionResponse;

public interface StructuredOperation extends BtreeOperation<StructuredActionResponse> {
    StructuredAction serialize();

    long getStoreId();

}
