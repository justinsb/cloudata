package com.cloudata.structured.sql.simple;

import java.util.List;
import java.util.Map;

import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.sql.planner.Symbol;

public class SimpleTableScan extends SimpleNode {
    RecordSet recordset;

    RecordCursor cursor;

    public List<String> columnNames;
    public List<SimpleExpression> expressions;

    SimpleColumnExpression[] columns;

    Map<Symbol, SimpleExpression> symbolToExpression;

    final TableMetadata tableMetadata;

    public SimpleTableScan(TableMetadata tableMetadata) {
        this.tableMetadata = tableMetadata;
    }

    public SimpleExpression getExpression(Symbol symbol) {
        return symbolToExpression.get(symbol);
    }

    public String getTableName() {
        return tableMetadata.getTable().getTableName();
    }

    @Override
    public <C, R> R accept(SimpleNodeVisitor<C, R> visitor, C context) {
        return visitor.visitTableScan(this, context);
    }
}