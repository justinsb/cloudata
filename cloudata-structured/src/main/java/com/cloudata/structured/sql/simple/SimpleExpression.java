package com.cloudata.structured.sql.simple;

public abstract class SimpleExpression {

    public abstract <C, R> R accept(SimpleExpressionVisitor<C, R> visitor, C context);

}
