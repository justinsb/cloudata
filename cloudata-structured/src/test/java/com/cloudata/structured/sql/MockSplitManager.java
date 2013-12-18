package com.cloudata.structured.sql;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class MockSplitManager implements ConnectorSplitManager {

    final String connectorId;

    public MockSplitManager(String connectorId) {
        this.connectorId = connectorId;
    }

    @Override
    public String getConnectorId() {
        return connectorId;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle) {
        return tableHandle instanceof MockTableHandle
                && ((MockTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public PartitionResult getPartitions(TableHandle tableHandle, TupleDomain tupleDomain) {
        checkArgument(tableHandle instanceof MockTableHandle, "tableHandle is not an instance of MockTableHandle");
        MockTableHandle exampleTableHandle = (MockTableHandle) tableHandle;

        // example connector has only one partition
        List<Partition> partitions = ImmutableList.<Partition> of(new MockPartition(exampleTableHandle.getSchemaName(),
                exampleTableHandle.getTableName()));
        // example connector does not do any additional processing/filtering with the TupleDomain, so just return the
        // whole TupleDomain
        ;
        return new PartitionResult(partitions, tupleDomain);
    }

    @Override
    public Iterable<Split> getPartitionSplits(TableHandle tableHandle, List<Partition> partitions) {
        checkNotNull(partitions, "partitions is null");
        checkArgument(partitions.size() == 1, "Expected one partition but got %s", partitions.size());
        Partition partition = partitions.get(0);

        checkArgument(partition instanceof MockPartition, "partition is not an instance of MockPartition");
        MockPartition examplePartition = (MockPartition) partition;

        MockTableHandle exampleTableHandle = (MockTableHandle) tableHandle;
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
        splits.add(new MockSplit(connectorId, examplePartition.getSchemaName(), examplePartition.getTableName(), URI
                .create("http://localhost:1234")));
        Collections.shuffle(splits);

        return splits;
    }

}
