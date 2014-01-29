package com.cloudata.keyvalue;

import java.io.Serializable;

import com.cloudata.DeepCopy;
import com.cloudata.cluster.GossipConfig;
import com.google.common.net.HostAndPort;

public class KeyValueConfig implements Serializable {
    public HostAndPort redisEndpoint;
    public HostAndPort protobufEndpoint;

    public int httpPort;

    public GossipConfig gossip;

    public KeyValueConfig deepCopy() {
        return DeepCopy.deepCopy(this);
    }

}