package com.cloudata.structured.sql;

import java.util.Collections;
import java.util.Set;

import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.google.common.collect.Sets;

public class MockNodeManager implements NodeManager {

    final Node me;
    final AllNodes allNodes;

    public MockNodeManager(Node me) {
        this.me = me;
        this.allNodes = new AllNodes(Sets.newHashSet(me), Collections.<Node> emptySet());
    }

    @Override
    public Set<Node> getActiveDatasourceNodes(String datasourceName) {
        return Sets.newHashSet(me);
    }

    @Override
    public AllNodes getAllNodes() {
        return allNodes;
    }

    @Override
    public Node getCurrentNode() {
        return me;
    }

    @Override
    public void refreshNodes() {
        throw new UnsupportedOperationException();
    }

}
