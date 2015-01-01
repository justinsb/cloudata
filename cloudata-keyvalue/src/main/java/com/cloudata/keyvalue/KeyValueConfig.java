package com.cloudata.keyvalue;

import java.io.Serializable;

import org.robotninjas.barge.ClusterConfig;

import com.cloudata.DeepCopy;
import com.cloudata.cluster.GossipConfig;
import com.google.common.net.HostAndPort;

public class KeyValueConfig implements Serializable {
  private static final long serialVersionUID = 1L;
  
    public HostAndPort redisEndpoint;
    public HostAndPort protobufEndpoint;

    public int httpPort;

    public GossipConfig gossip;

    public ClusterConfig seedConfig;
    
    public KeyValueConfig deepCopy() {
        return DeepCopy.deepCopy(this);
    }

}