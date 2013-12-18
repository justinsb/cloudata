package com.cloudata.structured.sql.provider;

import java.util.List;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.Split;

public class CloudataConnectorRecordSetProvider implements ConnectorRecordSetProvider {

    @Override
    public boolean canHandle(Split split) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns) {
        throw new UnsupportedOperationException();
    }

}
