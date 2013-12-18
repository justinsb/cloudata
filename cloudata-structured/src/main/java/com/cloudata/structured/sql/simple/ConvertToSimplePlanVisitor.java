package com.cloudata.structured.sql.simple;

import java.util.List;
import java.util.Map;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ConvertToSimplePlanVisitor extends PlanVisitor<Object, SimpleNode> {

    final Metadata metadata;

    public ConvertToSimplePlanVisitor(Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    protected SimpleNode visitPlan(PlanNode node, Object context) {
        // Not simple!
        return null;
    }

    // @Override
    // public SimpleNode visitProject(ProjectNode node, Integer context) {
    // SimpleNode source = node.getSource().accept(this, context);
    // if (source == null) {
    // return null;
    // }
    //
    // if (source instanceof SimpleTableScan) {
    // SimpleTableScan simpleTableScan = (SimpleTableScan) source;
    //
    // for (Map.Entry<Symbol, Expression> entry : node.getOutputMap().entrySet()) {
    // entry.getValue().accept(visitor, context);
    // if (entry.getValue() instanceof QualifiedNameReference && ((QualifiedNameReference)
    // entry.getValue()).getName().equals(entry.getKey().toQualifiedName())) {
    // // skip identity assignments
    // continue;
    // }
    // print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
    // }
    //
    // return processChildren(node, indent + 1);
    //
    //
    // // print(indent, "- Project => [%s]", formatOutputs(node.getOutputSymbols()));
    // // for (Map.Entry<Symbol, Expression> entry : node.getOutputMap().entrySet()) {
    // // if (entry.getValue() instanceof QualifiedNameReference && ((QualifiedNameReference)
    // entry.getValue()).getName().equals(entry.getKey().toQualifiedName())) {
    // // // skip identity assignments
    // // continue;
    // // }
    // // print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
    // // }
    // //
    // // return processChildren(node, indent + 1);
    // / - Project => [key1:varchar, concat:varchar, expr:varchar]
    // // concat := concat("key2", 'hello')
    // // expr := 'world'
    // } else {
    // throw new IllegalStateException();
    // }
    // }

    @Override
    public SimpleNode visitOutput(OutputNode node, Object context) {
        assert node.getSources().size() == 1;

        SimpleNode source = node.getSource().accept(this, context);
        if (source == null) {
            return null;
        }

        if (source instanceof SimpleTableScan) {
            SimpleTableScan tableScan = (SimpleTableScan) source;
            List<String> columnNames = node.getColumnNames();
            List<Symbol> outputSymbols = node.getOutputSymbols();

            List<SimpleExpression> expressions = Lists.newArrayList();
            for (int i = 0; i < columnNames.size(); i++) {
                // String name = columnNames.get(i);
                Symbol symbol = outputSymbols.get(i);

                SimpleExpression expression = tableScan.getExpression(symbol);
                if (expression == null) {
                    assert false;
                    return null;
                }
                expressions.add(expression);
            }

            tableScan.expressions = expressions;
            tableScan.columnNames = columnNames;
            return tableScan;
        } else {
            throw new IllegalStateException();
        }

        // print(indent, "- Output[%s]", Joiner.on(", ").join(node.getColumnNames()));
        // for (int i = 0; i < node.getColumnNames().size(); i++) {
        // String name = node.getColumnNames().get(i);
        // Symbol symbol = node.getOutputSymbols().get(i);
        // if (!name.equals(symbol.toString())) {
        // print(indent + 2, "%s := %s", name, symbol);
        // }
        // }
        //
        // return processChildren(node, indent + 1);
    }

    @Override
    public SimpleNode visitTableScan(TableScanNode node, Object context) {
        assert node.getSources().size() == 0;

        TupleDomain partitionsDomainSummary = node.getPartitionsDomainSummary();
        if (!partitionsDomainSummary.isAll()) {
            return null;
        }

        // plan: - Output[k1, k2, k3]
        // k1 := key1
        // k2 := concat
        // k3 := expr
        // - Project => [key1:varchar, concat:varchar, expr:varchar]
        // concat := concat("key2", 'hello')
        // expr := 'world'
        // - TableScan[com.cloudata.structured.sql.MockTableHandle@5850abcc, domain={}] => [key1:varchar,
        // key2:varchar]
        // key1 := com.cloudata.structured.sql.MockColumnHandle@56300388
        // key2 := com.cloudata.structured.sql.MockColumnHandle@6a3801ec

        TableHandle tableHandle = node.getTable();
        TableMetadata table = metadata.getTableMetadata(tableHandle);
        // SchemaTableName schemaTable = table.getTable();
        //
        // TableEntry entry = new TableEntry(table.getConnectorId(), schemaTable.getSchemaName(),
        // schemaTable.getTableName());
        String tableName = table.getTable().getTableName();

        Map<Symbol, SimpleExpression> symbolToExpression = Maps.newHashMap();
        List<String> columnNames = Lists.newArrayList();
        List<SimpleColumnExpression> expressions = Lists.newArrayList();

        for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
            Symbol symbol = entry.getKey();
            String name = symbol.getName();
            columnNames.add(name);

            ColumnHandle columnHandle = entry.getValue();

            ColumnMetadata columnMetadata = metadata.getColumnMetadata(tableHandle, columnHandle);
            if (columnMetadata == null) {
                assert false;
                return null;
            }

            SimpleColumnExpression expression = new SimpleColumnExpression(tableName, columnMetadata);
            expressions.add(expression);
            symbolToExpression.put(symbol, expression);

            // QualifiedNameReference expression = new QualifiedNameReference(entry.getKey().toQualifiedName());
            //
            // if (node.getOutputSymbols().contains(entry.getKey())
            // || (!partitionsDomainSummary.isNone() && partitionsDomainSummary.getDomains().keySet()
            // .contains(entry.getValue()))) {
            // print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
            // }
        }

        SimpleTableScan tableScan = new SimpleTableScan(table);
        tableScan.columnNames = columnNames;
        tableScan.expressions = Lists.<SimpleExpression> newArrayList(expressions);
        tableScan.symbolToExpression = symbolToExpression;
        tableScan.columns = expressions.toArray(new SimpleColumnExpression[expressions.size()]);

        return tableScan;
    }

}