package com.cloudata.structured.sql;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.TablePartition;
import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Optional;
import com.google.common.collect.Multimap;

public class MockShardManager implements ShardManager {

    @Override
    public void disassociateShard(long shardId, String nodeIdentifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropShard(long shardId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitPartition(TableHandle tableHandle, String partition, List<? extends PartitionKey> partitionKeys,
            Map<UUID, String> shards) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<TablePartition> getPartitions(TableHandle tableHandle) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Multimap<String, ? extends PartitionKey> getAllPartitionKeys(TableHandle tableHandle) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Multimap<Long, Entry<UUID, String>> getShardNodesByPartition(TableHandle tableHandle) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getTableNodes(TableHandle tableHandle) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<String> getAllNodesInUse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(TableHandle tableHandle, String partitionName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropOrphanedPartitions() {
        throw new UnsupportedOperationException();

    }

    @Override
    public Iterable<Long> getOrphanedShardIds(Optional<String> nodeIdentifier) {
        throw new UnsupportedOperationException();
    }

}
