package com.cloudata.structured.sql.provider;

import java.util.List;

import com.cloudata.btree.Keyspace;
import com.cloudata.structured.StructuredStore;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordSet;

public class CloudataRecordSet implements RecordSet {

    private final StructuredStore store;
    private final CloudataTableHandle tableHandle;

    public CloudataRecordSet(StructuredStore store, CloudataTableHandle tableHandle) {
        this.store = store;
        this.tableHandle = tableHandle;
    }

    @Override
    public List<ColumnType> getColumnTypes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CloudataRecordCursor cursor() {
        return new CloudataRecordCursor(store, tableHandle);
    }

    public Keyspace getKeyspace() {
        return tableHandle.getKeyspace();
    }

}
