package com.cloudata.structured.sql.provider;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.List;

import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class CloudataSplitManager implements ConnectorSplitManager {
    final NodeManager nodeManager;
    final String connectorId;

    public CloudataSplitManager(NodeManager nodeManager, String connectorId) {
        this.nodeManager = nodeManager;
        this.connectorId = connectorId;
    }

    @Override
    public String getConnectorId() {
        return connectorId;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle) {
        return tableHandle instanceof CloudataTableHandle
                && ((CloudataTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public PartitionResult getPartitions(TableHandle table, TupleDomain tupleDomain) {
        // checkArgument(table instanceof CloudataTableHandle, "tableHandle is not an instance of CloudataTableHandle");
        assert table instanceof CloudataTableHandle;
        CloudataTableHandle tableHandle = (CloudataTableHandle) table;

        // example connector has only one partition
        List<Partition> partitions = ImmutableList.<Partition> of(new CloudataPartition(tableHandle.getSchemaName(),
                tableHandle.getTableName()));
        // example connector does not do any additional processing/filtering with the TupleDomain, so just return the
        // whole TupleDomain

        return new PartitionResult(partitions, tupleDomain);
    }

    @Override
    public Iterable<Split> getPartitionSplits(TableHandle table, List<Partition> partitions) {
        // checkNotNull(partitions, "partitions is null");
        assert partitions != null;
        checkArgument(partitions.size() == 1, "Expected one partition but got %s", partitions.size());

        // checkArgument(partition instanceof CloudataPartition, "partition is not an instance of CloudataPartition");
        assert partitions.get(0) instanceof CloudataPartition;
        CloudataPartition partition = (CloudataPartition) partitions.get(0);

        CloudataTableHandle tableHandle = (CloudataTableHandle) table;
        // ExampleTable table = exampleClient.getTable(exampleTableHandle.getSchemaName(),
        // exampleTableHandle.getTableName());
        // // this can happen if table is removed during a query
        // checkState(table != null, "Table %s.%s no longer exists", exampleTableHandle.getSchemaName(),
        // exampleTableHandle.getTableName());

        List<Split> splits = Lists.newArrayList();
        // for (URI uri : table.getSources()) {
        // splits.add(new ExampleSplit(connectorId, examplePartition.getSchemaName(), examplePartition.getTableName(),
        // uri));
        // }
        // URI.create("http://localhost:1234");

        splits.add(new CloudataSplit(connectorId, partition.getSchemaName(), partition.getTableName(), nodeManager
                .getCurrentNode().getHostAndPort()));
        Collections.shuffle(splits);

        return splits;
    }

}
