package com.cloudata.structured.operation;

import com.cloudata.btree.Keyspace;
import com.cloudata.structured.StructuredProtocol.StructuredAction;
import com.cloudata.structured.StructuredProtocol.StructuredActionResponse;
import com.cloudata.structured.StructuredProtocol.StructuredResponseEntry;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public abstract class StructuredOperationBase implements StructuredOperation {

    protected final StructuredAction action;

    protected final StructuredActionResponse.Builder response = StructuredActionResponse.newBuilder();

    protected final boolean returnValues;
    protected final boolean returnKeys;

    protected final ByteString qualifiedKey;

    protected final Keyspace keyspace;

    public StructuredOperationBase(StructuredAction action) {
        this.action = action;

        if (action.hasKey()) {
            Preconditions.checkState(action.hasKeyspaceId());
            this.keyspace = Keyspace.fromId(action.getKeyspaceId());
            this.qualifiedKey = keyspace.mapToKey(action.getKey());
        } else {
            this.keyspace = null;
            this.qualifiedKey = null;
        }

        Preconditions.checkArgument(action.hasStoreId());

        this.returnValues = !hasSuppressField(StructuredResponseEntry.VALUE_FIELD_NUMBER);
        this.returnKeys = !hasSuppressField(StructuredResponseEntry.KEY_FIELD_NUMBER);
    }

    private boolean hasSuppressField(int field) {
        if (action.getSuppressFieldsCount() == 0) {
            return false;
        }

        for (int i = 0; i < action.getSuppressFieldsCount(); i++) {
            if (action.getSuppressFields(i) == field) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public final StructuredAction serialize() {
        return action;
    }

    @Override
    public StructuredActionResponse getResult() {
        return response.build();
    }

    @Override
    public long getStoreId() {
        return action.getStoreId();
    }

}
