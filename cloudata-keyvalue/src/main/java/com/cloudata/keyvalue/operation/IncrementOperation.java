package com.cloudata.keyvalue.operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.Keyspace;
import com.cloudata.keyvalue.KeyValueProtocol.ActionType;
import com.cloudata.keyvalue.KeyValueProtocol.KeyValueAction;
import com.cloudata.keyvalue.KeyValueProtocol.ResponseEntry.Builder;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class IncrementOperation extends KeyValueOperationBase {

    private static final Logger log = LoggerFactory.getLogger(IncrementOperation.class);

    public IncrementOperation(KeyValueAction entry) {
        super(entry);

        Preconditions.checkArgument(entry.getAction() == ActionType.INCREMENT);
        Preconditions.checkArgument(entry.hasValue());
    }

    @Override
    public Value doAction(Value oldValue) {
        long oldValueLong = 0;

        if (oldValue != null) {
            oldValueLong = oldValue.asLong();
        }

        Value incrementBy = Value.fromRawBytes(entry.getValue());
        long newValueLong = oldValueLong + incrementBy.asLong();

        Value newValue = Value.fromLong(newValueLong);

        log.debug("Increment: {} -> {}", oldValueLong, newValueLong);

        // this.newValue = newValue.duplicate();

        Builder eb = response.addEntryBuilder();
        eb.setKey(entry.getKey());
        eb.setValue(ByteString.copyFrom(newValue.asBytes()));

        return newValue;
    }

    public static IncrementOperation build(long storeId, Keyspace keyspace, ByteString key, long delta) {
        KeyValueAction.Builder b = KeyValueAction.newBuilder();
        b.setAction(ActionType.INCREMENT);
        b.setStoreId(storeId);
        b.setKeyspaceId(keyspace.getKeyspaceId());
        b.setKey(key);
        b.setValue(ByteString.copyFrom(Value.fromLong(delta).asBytes()));
        return new IncrementOperation(b.build());
    }

}
