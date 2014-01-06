package com.cloudata.keyvalue.operation;

import java.nio.ByteBuffer;

import com.cloudata.btree.Keyspace;
import com.cloudata.btree.operation.RowOperation;
import com.cloudata.keyvalue.KeyValueProtocol.ActionResponse;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueAction;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public abstract class KeyValueOperationBase implements KeyValueOperation, RowOperation<ActionResponse> {

    protected final KeyValueAction entry;

    protected final ActionResponse.Builder response = ActionResponse.newBuilder();

    final ByteString qualifiedKey;

    public KeyValueOperationBase(KeyValueAction entry) {
        this.entry = entry;

        if (entry.hasKey()) {
            Preconditions.checkState(entry.hasKeyspaceId());
            this.qualifiedKey = Keyspace.user(entry.getKeyspaceId()).mapToKey(entry.getKey());
        } else {
            this.qualifiedKey = null;
        }

        Preconditions.checkArgument(entry.hasStoreId());
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public final KeyValueAction serialize() {
        return entry;
    }

    @Override
    public ActionResponse getResult() {
        return response.build();
    }

    @Override
    public ByteBuffer getKey() {
        return qualifiedKey.asReadOnlyByteBuffer();
    }

    @Override
    public long getStoreId() {
        return entry.getStoreId();
    }

}
