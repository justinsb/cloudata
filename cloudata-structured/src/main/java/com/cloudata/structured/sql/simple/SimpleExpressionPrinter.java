package com.cloudata.structured.sql.simple;

public class SimpleExpressionPrinter extends SimpleExpressionVisitor<Object, String> {

    @Override
    public String visitGeneric(SimpleExpression node, Object context) {
        return "(unknown:" + node.toString() + ")";
    }

    @Override
    public String visitColumnExpression(SimpleColumnExpression node, Object context) {
        return node.getTableName() + "." + node.getColumnName();
    }

    public static String toString(SimpleExpression node) {
        SimpleExpressionPrinter visitor = new SimpleExpressionPrinter();
        return node.accept(visitor, null);
    }
}
