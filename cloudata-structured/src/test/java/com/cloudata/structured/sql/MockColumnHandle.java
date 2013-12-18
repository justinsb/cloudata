package com.cloudata.structured.sql;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;

public class MockColumnHandle implements ColumnHandle {

    private final ColumnMetadata columnMetadata;

    public MockColumnHandle(MockTableHandle mockTableHandle, ColumnMetadata columnMetadata) {
        this.columnMetadata = columnMetadata;
    }

    public ColumnMetadata getColumnMetadata() {
        return columnMetadata;
    }

}
