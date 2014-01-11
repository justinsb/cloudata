package com.cloudata.structured.sql.provider;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weakref.jmx.com.google.common.collect.Lists;
import org.weakref.jmx.com.google.common.collect.Maps;

import com.cloudata.btree.ByteBuffers;
import com.cloudata.btree.Keyspace;
import com.cloudata.structured.Listener;
import com.cloudata.structured.StructuredStore;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Charsets;

public class CloudataTableHandle implements TableHandle {

    private static final Logger log = LoggerFactory.getLogger(CloudataTableHandle.class);

    private final String tableName;
    private final ConnectorTableMetadata tableMetadata;
    private final List<ColumnMetadata> columns;
    private final String connectorId;
    private final String schemaName;

    private final Keyspace keyspace;

    private final StructuredStore store;

    public CloudataTableHandle(StructuredStore store, String connectorId, String schemaName, String tableName,
            Keyspace keyspace) {
        this.store = store;
        this.connectorId = connectorId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.keyspace = keyspace;
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

        final List<ColumnMetadata> columns = Lists.newArrayList();

        store.getKeys().listKeys(keyspace, new Listener<ByteBuffer>() {
            @Override
            public boolean next(ByteBuffer value) {
                ColumnType type = ColumnType.STRING;
                boolean paritionKey = false;
                String key = ByteBuffers.toString(Charsets.UTF_8, value);
                columns.add(new ColumnMetadata(key, type, columns.size(), paritionKey));
                return true;
            }

            @Override
            public void done() {

            }
        });

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

    public Keyspace getKeyspace() {
        return keyspace;
    }

}
