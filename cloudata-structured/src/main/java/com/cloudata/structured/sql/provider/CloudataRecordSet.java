package com.cloudata.structured.sql.provider;

import java.util.List;

import com.cloudata.structured.StructuredStore;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;

public class CloudataRecordSet implements RecordSet {

    private final StructuredStore store;
    private final TableMetadata tableMetadata;

    public CloudataRecordSet(StructuredStore store, TableMetadata tableMetadata) {
        this.store = store;
        this.tableMetadata = tableMetadata;
    }

    @Override
    public List<ColumnType> getColumnTypes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordCursor cursor() {
        return new CloudataRecordCursor(store, tableMetadata);
    }

}
