package com.cloudata.structured.sql;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import org.weakref.jmx.com.google.common.collect.Lists;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;

public class MockConnectorMetadata extends ReadOnlyConnectorMetadata {

    final String connectorId;

    public MockConnectorMetadata(String connectorId) {
        this.connectorId = connectorId;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle) {
        return tableHandle instanceof MockTableHandle
                && ((MockTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public List<String> listSchemaNames() {
        List<String> schemas = Lists.newArrayList();
        schemas.add("default");
        return schemas;
    }

    @Override
    public MockTableHandle getTableHandle(SchemaTableName tableName) {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        // ExampleTable table = exampleClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        // if (table == null) {
        // return null;
        // }

        return new MockTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(TableHandle table) {
        checkArgument(table instanceof MockTableHandle, "tableHandle is not an instance of MockTableHandle");
        MockTableHandle exampleTableHandle = (MockTableHandle) table;
        checkArgument(exampleTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        return exampleTableHandle.getTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(String schemaNameOrNull) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName) {
        MockTableHandle exampleTableHandle = promote(tableHandle);

        // ExampleTable table = exampleClient.getTable(exampleTableHandle.getSchemaName(),
        // exampleTableHandle.getTableName());
        // if (table == null) {
        // throw new TableNotFoundException(exampleTableHandle.toSchemaTableName());
        // }

        // ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        // for (ColumnMetadata columnMetadata : table.getColumnsMetadata()) {
        // columnHandles.put(columnMetadata.getName(), new ExampleColumnHandle(connectorId, columnMetadata));
        // }
        // return columnHandles.build();

        return exampleTableHandle.getColumnHandle(columnName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle) {
        MockTableHandle exampleTableHandle = promote(tableHandle);

        // ExampleTable table = exampleClient.getTable(exampleTableHandle.getSchemaName(),
        // exampleTableHandle.getTableName());
        // if (table == null) {
        // throw new TableNotFoundException(exampleTableHandle.toSchemaTableName());
        // }

        // ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        // for (ColumnMetadata columnMetadata : table.getColumnsMetadata()) {
        // columnHandles.put(columnMetadata.getName(), new ExampleColumnHandle(connectorId, columnMetadata));
        // }
        // return columnHandles.build();

        return exampleTableHandle.getColumnHandles();
    }

    private MockTableHandle promote(TableHandle tableHandle) {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof MockTableHandle, "tableHandle is not an instance of MockTableHandle");
        MockTableHandle exampleTableHandle = (MockTableHandle) tableHandle;
        checkArgument(exampleTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        return exampleTableHandle;
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle) {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(tableHandle instanceof MockTableHandle, "tableHandle is not an instance of MockTableHandle");
        checkArgument(((MockTableHandle) tableHandle).getConnectorId().equals(connectorId),
                "tableHandle is not for this connector");
        checkArgument(columnHandle instanceof MockColumnHandle,
                "columnHandle is not an instance of ExampleColumnHandle");

        return ((MockColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix) {
        throw new UnsupportedOperationException();
    }

}
