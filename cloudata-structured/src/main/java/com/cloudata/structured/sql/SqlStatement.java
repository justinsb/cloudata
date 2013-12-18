package com.cloudata.structured.sql;

import com.cloudata.structured.sql.simple.ConvertToSimplePlanVisitor;
import com.cloudata.structured.sql.simple.SimpleNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.Plan;
import com.google.common.base.Optional;

public class SqlStatement {

    private final String sql;
    private final Plan plan;

    Optional<SimpleNode> simple;
    private final Metadata metadata;

    public SqlStatement(Metadata metadata, String sql, Plan plan) {
        this.metadata = metadata;
        this.sql = sql;
        this.plan = plan;
    }

    public boolean isSimple() {
        return getSimple() != null;
    }

    public SimpleNode getSimple() {
        if (simple == null) {
            ConvertToSimplePlanVisitor visitor = new ConvertToSimplePlanVisitor(metadata);
            SimpleNode accept = plan.getRoot().accept(visitor, null);
            // plan.getRoot().accept(visitor, 0);
            simple = Optional.fromNullable(accept);
        }
        return simple.orNull();
    }

    // class SimplePlan {
    // final Table table;
    //
    // final Expression[] expressions;
    //
    // public void evaluate() {
    // // Statement: Query{queryBody=QuerySpecification{select=Select{distinct=false, selectItems=["key1" k1,
    // // concat("key2", 'hello') k2]}, from=[Table{table1}], where=null, groupBy=[], having=null, orderBy=[],
    // // limit=null}, orderBy=[]}
    // // analysis: com.facebook.presto.sql.analyzer.Analysis@39f46204
    // // plan: - Output[k1, k2]
    // // k1 := key1
    // // k2 := concat
    // // - Project => [key1:varchar, concat:varchar]
    // // concat := concat("key2", 'hello')
    // // - TableScan[com.cloudata.structured.sql.MockTableHandle@737c45ee, domain={}] => [key1:varchar,
    // // key2:varchar]
    // // key1 := com.cloudata.structured.sql.MockColumnHandle@549448df
    // // key2 := com.cloudata.structured.sql.MockColumnHandle@533c53da
    //
    // // ExpressionInterpreter.expressionInterpreter(expression, metadata, session)
    //
    // }
    // }
    //
    // private boolean isSimple(Plan plan) {
    //
    // // TODO: Make this better, once we have a better grip on the logic
    // PlanNode root = plan.getRoot();
    // if (root instanceof OutputNode) {
    // OutputNode outputNode = (OutputNode) root;
    // PlanNode source = outputNode.getSource();
    // if (source instanceof TableScanNode) {
    // TableScanNode tableScanNode = (TableScanNode) source;
    //
    // List<String> columns = Lists.newArrayList();
    // // List<String> columns = Lists.newArrayList();
    //
    // for (int i = 0; i < outputNode.getColumnNames().size(); i++) {
    // String name = outputNode.getColumnNames().get(i);
    // Symbol symbol = outputNode.getOutputSymbols().get(i);
    //
    // }
    //
    // // Statement: Query{queryBody=QuerySpecification{select=Select{distinct=false, selectItems=["key1" k1,
    // // "key2" k2]}, from=[Table{table1}], where=null, groupBy=[], having=null, orderBy=[], limit=null},
    // // orderBy=[]}
    // // analysis: com.facebook.presto.sql.analyzer.Analysis@6fcc5b5d
    // // plan: - Output[k1, k2]
    // // k1 := key1
    // // k2 := key2
    // // - TableScan[com.cloudata.structured.sql.MockTableHandle@3aa92b03, domain={}] => [key1:varchar,
    // // key2:varchar]
    // // key1 := com.cloudata.structured.sql.MockColumnHandle@20bb82ca
    // // key2 := com.cloudata.structured.sql.MockColumnHandle@7687ac8f
    //
    // SimpleQuery query = new SimpleQuery();
    // return true;
    // }
    // }
    // return false;
    // }

    // class CheckSimpleVisitor extends RecursivePlanVisitor<Integer, Boolean> {
    // boolean simple = true;
    //
    // @Override
    // public Boolean visitOutput(OutputNode node, Integer context) {
    // simple = false;
    // return false;
    // }
    //
    // @Override
    // protected Boolean visitPlan(PlanNode node, Integer context) {
    // if (vi)
    // }
    //
    // }

}
