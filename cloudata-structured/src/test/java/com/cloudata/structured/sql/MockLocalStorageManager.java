package com.cloudata.structured.sql;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.ColumnFileHandle;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.spi.ColumnHandle;

public class MockLocalStorageManager implements LocalStorageManager {

    @Override
    public BlockIterable getBlocks(UUID shardUuid, ColumnHandle columnHandle) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean shardExists(UUID shardUuid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropShard(UUID shardUuid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isShardActive(UUID shardUuid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnFileHandle createStagingFileHandles(UUID shardUuid, List<? extends ColumnHandle> columnHandles)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(ColumnFileHandle columnFileHandle) throws IOException {
        throw new UnsupportedOperationException();
    }

}
