package com.cloudata.structured.sql.provider;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weakref.jmx.com.google.common.collect.Lists;
import org.weakref.jmx.com.google.common.collect.Maps;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;

public class CloudataTableHandle implements TableHandle {

    private static final Logger log = LoggerFactory.getLogger(CloudataTableHandle.class);

    private final String tableName;
    private final ConnectorTableMetadata tableMetadata;
    private final List<ColumnMetadata> columns;
    private final String connectorId;
    private final String schemaName;

    public CloudataTableHandle(String connectorId, String schemaName, String tableName) {
        this.connectorId = connectorId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

        List<String> keys = Lists.newArrayList();
        log.error("Keys are hard-coded");
        keys.add("column1");
        keys.add("column2");
        keys.add("column3");

        List<ColumnMetadata> columns = Lists.newArrayList();
        for (int i = 0; i < keys.size(); i++) {
            ColumnType type = ColumnType.STRING;
            boolean paritionKey = false;
            columns.add(new ColumnMetadata(keys.get(i), type, i, paritionKey));
        }
        this.columns = columns;
        ConnectorTableMetadata metadata = new ConnectorTableMetadata(schemaTableName, columns);
        this.tableMetadata = metadata;
        // new TableMetadata(connectorId, metadata);
    }

    public ConnectorTableMetadata getTableMetadata() {
        return tableMetadata;
    }

    public Map<String, ColumnHandle> getColumnHandles() {
        Map<String, ColumnHandle> handles = Maps.newHashMap();
        for (ColumnMetadata column : columns) {
            handles.put(column.getName(), new CloudataColumnHandle(this, column));
        }
        return handles;
    }

    public String getConnectorId() {
        return connectorId;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public ColumnHandle getColumnHandle(String columnName) {
        return getColumnHandles().get(columnName);
    }

}
