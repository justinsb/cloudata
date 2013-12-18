package com.cloudata.structured.sql.simple;

public abstract class SimpleNode {

    public abstract <C, R> R accept(SimpleNodeVisitor<C, R> visitor, C context);
}
