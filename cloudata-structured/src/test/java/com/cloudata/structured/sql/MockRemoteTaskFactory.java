package com.cloudata.structured.sql;

import java.util.Map;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.spi.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.OutputReceiver;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.Multimap;

public class MockRemoteTaskFactory implements RemoteTaskFactory {

    @Override
    public RemoteTask createRemoteTask(Session session, TaskId taskId, Node node, PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits, Map<PlanNodeId, OutputReceiver> outputReceivers,
            OutputBuffers outputBuffers) {
        throw new UnsupportedOperationException();
    }

}
