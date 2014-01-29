package com.cloudata.cluster;

import java.io.Serializable;

import com.cloudata.DeepCopy;
import com.google.common.net.HostAndPort;

public class GossipConfig implements Serializable {
    public HostAndPort protobufEndpoint;
    public HostAndPort broadcastAddress;

    public String serviceId;
    public String nodeId;

    public GossipConfig deepCopy() {
        return DeepCopy.deepCopy(this);
    }
}
