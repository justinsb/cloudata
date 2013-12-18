package com.cloudata.structured.sql;

import com.facebook.presto.sql.planner.plan.TableScanNode;

public class TableScanCountVisitor extends RecursivePlanVisitor<Integer, Integer> {

    int count;

    @Override
    public Integer visitTableScan(TableScanNode node, Integer context) {
        count++;
        return super.visitTableScan(node, context);
    }

}
