package com.cloudata.structured.sql;

import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MaterializedViewWriterNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;

public class RecursivePlanVisitor<C, R> extends PlanVisitor<C, R> {

    // @Override
    // protected R visitPlan(PlanNode node, C context) {
    // return super.visitPlan(node, context);
    // }

    @Override
    public R visitExchange(ExchangeNode node, C context) {
        return super.visitExchange(node, context);
    }

    @Override
    public R visitAggregation(AggregationNode node, C context) {
        node.getSource().accept(this, context);

        return super.visitAggregation(node, context);
    }

    @Override
    public R visitFilter(FilterNode node, C context) {
        node.getSource().accept(this, context);

        return super.visitFilter(node, context);
    }

    @Override
    public R visitProject(ProjectNode node, C context) {
        node.getSource().accept(this, context);

        return super.visitProject(node, context);
    }

    @Override
    public R visitTopN(TopNNode node, C context) {
        node.getSource().accept(this, context);

        return super.visitTopN(node, context);
    }

    @Override
    public R visitOutput(OutputNode node, C context) {
        node.getSource().accept(this, context);

        return super.visitOutput(node, context);
    }

    @Override
    public R visitLimit(LimitNode node, C context) {
        node.getSource().accept(this, context);

        return super.visitLimit(node, context);
    }

    @Override
    public R visitSample(SampleNode node, C context) {
        node.getSource().accept(this, context);

        return super.visitSample(node, context);
    }

    @Override
    public R visitTableScan(TableScanNode node, C context) {
        return super.visitTableScan(node, context);
    }

    @Override
    public R visitJoin(JoinNode node, C context) {
        node.getLeft().accept(this, context);
        node.getRight().accept(this, context);

        return super.visitJoin(node, context);
    }

    @Override
    public R visitSemiJoin(SemiJoinNode node, C context) {
        node.getSource().accept(this, context);
        node.getFilteringSource().accept(this, context);

        return super.visitSemiJoin(node, context);
    }

    @Override
    public R visitSort(SortNode node, C context) {
        node.getSource().accept(this, context);

        return super.visitSort(node, context);
    }

    @Override
    public R visitSink(SinkNode node, C context) {
        node.getSource().accept(this, context);

        return super.visitSink(node, context);
    }

    @Override
    public R visitWindow(WindowNode node, C context) {
        node.getSource().accept(this, context);

        return super.visitWindow(node, context);
    }

    @Override
    public R visitMaterializedViewWriter(MaterializedViewWriterNode node, C context) {
        node.getSource().accept(this, context);

        return super.visitMaterializedViewWriter(node, context);
    }

    @Override
    public R visitUnion(UnionNode node, C context) {
        for (PlanNode subPlanNode : node.getSources()) {
            subPlanNode.accept(this, context);
        }
        return super.visitUnion(node, context);
    }

}
