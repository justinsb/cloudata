package com.cloudata.structured.operation;

import java.nio.ByteBuffer;

import com.cloudata.btree.operation.RowOperation;
import com.cloudata.structured.StructuredProtocol.StructuredAction;
import com.cloudata.structured.StructuredProtocol.StructuredActionResponse;

public abstract class SimpleStructuredOperationBase extends StructuredOperationBase implements
        RowOperation<StructuredActionResponse> {

    public SimpleStructuredOperationBase(StructuredAction action) {
        super(action);
    }

    @Override
    public ByteBuffer getKey() {
        return qualifiedKey.asReadOnlyByteBuffer();
    }

}
