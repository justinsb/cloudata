package com.cloudata.structured.sql.simple;

import com.cloudata.structured.sql.value.ValueHolder;
import com.facebook.presto.spi.ColumnMetadata;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class SimpleColumnExpression extends SimpleExpression {
    private final String tableName;
    private final ColumnMetadata columnMetadata;
    public ValueHolder valueHolder;

    final String[] path;

    public SimpleColumnExpression(String tableName, ColumnMetadata columnMetadata) {
        this.tableName = tableName;
        this.columnMetadata = columnMetadata;

        this.valueHolder = new ValueHolder();

        this.path = columnMetadata.getName().split("\\.");
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

    @Override
    public void evalute(ValueHolder dest) {
        dest.copyFrom(valueHolder);
    }

    public void update(JsonObject json) {
        JsonElement value;
        if (path.length == 1) {
            value = json.get(path[0]);
        } else {
            JsonObject current = json;

            value = null;

            for (int i = 0; i < path.length; i++) {
                if (i != 0) {
                    if (!value.isJsonObject()) {
                        value = null;
                        break;
                    }
                    current = (JsonObject) value;
                }
                value = current.get(path[i]);
                if (value == null) {
                    break;
                }
            }
        }

        if (value == null) {
            setValueNull();
        } else if (value.isJsonPrimitive()) {
            setValue((JsonPrimitive) value);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private void setValue(JsonPrimitive value) {
        if (value.isBoolean()) {
            valueHolder.set(value.getAsBoolean());
        } else if (value.isString()) {
            valueHolder.set(value.getAsString());
        } else if (value.isNumber()) {
            Number v = value.getAsNumber();
            Class<? extends Number> c = v.getClass();

            if (c == Integer.class || c == Long.class || c == Short.class) {
                long n = v.longValue();
                valueHolder.set(n);
            } else {
                assert c == Float.class || c == Double.class;
                double n = v.doubleValue();
                valueHolder.set(n);
            }
        } else {
            throw new IllegalStateException();
        }
    }

    private void setValueNull() {
        valueHolder.setNull();
    }

}
