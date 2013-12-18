package com.cloudata.structured.sql.simple;

import com.facebook.presto.spi.ColumnMetadata;

public class SimpleColumnExpression extends SimpleExpression {
    private final String tableName;
    private final ColumnMetadata columnMetadata;

    public SimpleColumnExpression(String tableName, ColumnMetadata columnMetadata) {
        this.tableName = tableName;
        this.columnMetadata = columnMetadata;
    }

    public String getColumnName() {
        return columnMetadata.getName();
    }

    @Override
    public <C, R> R accept(SimpleExpressionVisitor<C, R> visitor, C context) {
        return visitor.visitColumnExpression(this, context);
    }

    public String getTableName() {
        return tableName;
    }

}
