package com.cloudata.structured.operation;

import com.cloudata.btree.Btree;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.btree.operation.ComplexOperation;
import com.cloudata.btree.operation.DeleteOperation;
import com.cloudata.structured.StructuredProto.LogAction;
import com.cloudata.structured.StructuredProto.LogEntry;
import com.cloudata.structured.StructuredStore;
import com.google.protobuf.ByteString;

public class StructuredDeleteOperation implements StructuredOperation<Integer>, ComplexOperation<Integer> {

    private final StructuredStore store;
    private final ByteString keyspaceName;
    private final ByteString key;

    int deleteCount;

    public StructuredDeleteOperation(StructuredStore store, ByteString keyspaceName, ByteString key) {
        this.store = store;
        this.keyspaceName = keyspaceName;
        this.key = key;
    }

    @Override
    public LogEntry.Builder serialize() {
        LogEntry.Builder b = LogEntry.newBuilder();
        b.setKey(key);
        b.setKeyspaceName(keyspaceName);
        b.setAction(LogAction.DELETE);
        return b;
    }

    @Override
    public Integer getResult() {
        return deleteCount;
    }

    @Override
    public void doAction(Btree btree, WriteTransaction txn) {
        Keyspace keyspace = store.findKeyspace(txn, keyspaceName);
        if (keyspace != null) {
            ByteString qualifiedKey = keyspace.mapToKey(key);

            // Set the value
            DeleteOperation op = new DeleteOperation(qualifiedKey);
            txn.doAction(btree, op);
            deleteCount += op.getResult();
        }
    }
}
