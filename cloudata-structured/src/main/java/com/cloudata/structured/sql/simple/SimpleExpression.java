package com.cloudata.structured.sql.simple;

import com.cloudata.structured.sql.value.ValueHolder;

public abstract class SimpleExpression {

    public abstract <C, R> R accept(SimpleExpressionVisitor<C, R> visitor, C context);

    public abstract void evalute(ValueHolder valueHolder);
}
