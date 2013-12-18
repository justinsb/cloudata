package com.cloudata.structured.sql;

import java.net.URI;

import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.metadata.Node;

public class MockLocationFactory implements LocationFactory {

    // private final URI baseUri;
    // private final NodeManager nodeManager;
    //
    // public MockLocationFactory(NodeManager nodeManager, URI baseUri) {
    // this.nodeManager = nodeManager;
    // this.baseUri = baseUri;
    // }

    // @Override
    // public URI createQueryLocation(QueryId queryId) {
    // Preconditions.checkNotNull(queryId, "queryId is null");
    // return uriBuilderFrom(baseUri).appendPath("/v1/query").appendPath(queryId.toString()).build();
    // }
    //
    // @Override
    // public URI createStageLocation(StageId stageId) {
    // Preconditions.checkNotNull(stageId, "stageId is null");
    // return uriBuilderFrom(baseUri).appendPath("v1/stage").appendPath(stageId.toString()).build();
    // }
    //
    // @Override
    // public URI createLocalTaskLocation(TaskId taskId) {
    // return createTaskLocation(nodeManager.getCurrentNode(), taskId);
    // }
    //
    // @Override
    // public URI createTaskLocation(Node node, TaskId taskId) {
    // Preconditions.checkNotNull(node, "node is null");
    // Preconditions.checkNotNull(taskId, "taskId is null");
    // return uriBuilderFrom(node.getHttpUri()).appendPath("/v1/task").appendPath(taskId.toString()).build();
    // }

    @Override
    public URI createQueryLocation(QueryId queryId) {
        return URI.create("fake://query/" + queryId);
    }

    @Override
    public URI createStageLocation(StageId stageId) {
        return URI.create("fake://stage/" + stageId);
    }

    @Override
    public URI createLocalTaskLocation(TaskId taskId) {
        return URI.create("fake://task/" + taskId);
    }

    @Override
    public URI createTaskLocation(Node node, TaskId taskId) {
        return URI.create("fake://task/" + node.getNodeIdentifier() + "/" + taskId);
    }

}
