package com.cloudata.structured.sql.simple;

public class SimpleExpressionVisitor<C, R> {

    public R visitGeneric(SimpleExpression node, C context) {
        return null;
    }

    public R visitColumnExpression(SimpleColumnExpression node, C context) {
        return visitGeneric(node, context);
    }
}
