package com.cloudata.structured.sql.provider;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;

public class CloudataConnectorHandleResolver implements ConnectorHandleResolver {

    @Override
    public boolean canHandle(TableHandle tableHandle) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canHandle(Split split) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends Split> getSplitClass() {
        throw new UnsupportedOperationException();
    }

}
