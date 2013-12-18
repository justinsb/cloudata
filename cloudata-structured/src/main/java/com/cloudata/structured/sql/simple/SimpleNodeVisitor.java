package com.cloudata.structured.sql.simple;

public class SimpleNodeVisitor<C, R> {
    public R visitGeneric(SimpleNode node, C context) {
        return null;
    }

    public R visitTableScan(SimpleTableScan node, C context) {
        return visitGeneric(node, context);
    }
}
