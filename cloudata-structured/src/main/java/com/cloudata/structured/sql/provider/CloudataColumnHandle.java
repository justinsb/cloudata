package com.cloudata.structured.sql.provider;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;

public class CloudataColumnHandle implements ColumnHandle {

    private final ColumnMetadata columnMetadata;

    public CloudataColumnHandle(CloudataTableHandle tableHandle, ColumnMetadata columnMetadata) {
        this.columnMetadata = columnMetadata;
    }

    public ColumnMetadata getColumnMetadata() {
        return columnMetadata;
    }

}
