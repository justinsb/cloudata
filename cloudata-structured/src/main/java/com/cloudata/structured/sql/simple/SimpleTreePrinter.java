package com.cloudata.structured.sql.simple;

import static java.lang.String.format;

import com.google.common.base.Strings;

public class SimpleTreePrinter extends SimpleNodeVisitor<Integer, Void> {
    private final StringBuilder output = new StringBuilder();

    @Override
    public Void visitGeneric(SimpleNode node, Integer indent) {
        print(indent, "[Generic]: %s", node);
        return null;
    }

    @Override
    public Void visitTableScan(SimpleTableScan node, Integer indent) {
        print(indent, "[TableScan]: %s", node.getTableName());
        for (int i = 0; i < node.expressions.size(); i++) {
            SimpleExpression expression = node.expressions.get(i);
            String columnName = node.columnNames.get(i);

            print(indent + 2, "%s := %s", columnName, SimpleExpressionPrinter.toString(expression));
        }
        return null;
    }

    private void print(int indent, String format, Object... args) {
        String value;

        if (args.length == 0) {
            value = format;
        } else {
            value = format(format, args);
        }
        output.append(Strings.repeat("    ", indent)).append(value).append('\n');
    }

    public static String toString(SimpleNode simple) {
        SimpleTreePrinter printer = new SimpleTreePrinter();
        simple.accept(printer, 0);
        return printer.output.toString();
    }

}
