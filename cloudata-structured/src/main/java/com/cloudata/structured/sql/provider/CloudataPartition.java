package com.cloudata.structured.sql.provider;

import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Objects;

public class CloudataPartition implements Partition {

    private final String schemaName;
    private final String tableName;

    public CloudataPartition(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public String getPartitionId() {
        return schemaName + ":" + tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public TupleDomain getTupleDomain() {
        return TupleDomain.all();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("schemaName", schemaName).add("tableName", tableName).toString();
    }

}
