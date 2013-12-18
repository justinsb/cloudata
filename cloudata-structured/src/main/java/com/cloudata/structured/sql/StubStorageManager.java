package com.cloudata.structured.sql;

import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.storage.StorageManager;

public class StubStorageManager implements StorageManager {

    @Override
    public void insertTableSource(NativeTableHandle tableHandle, QualifiedTableName sourceTableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QualifiedTableName getTableSource(NativeTableHandle tableHandle) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTableSource(NativeTableHandle tableHandle) {
        throw new UnsupportedOperationException();

    }

}
