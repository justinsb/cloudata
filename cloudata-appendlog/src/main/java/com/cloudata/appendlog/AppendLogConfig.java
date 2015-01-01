package com.cloudata.appendlog;

import java.io.Serializable;

import org.robotninjas.barge.ClusterConfig;

import com.cloudata.DeepCopy;
import com.cloudata.cluster.GossipConfig;
import com.google.common.net.HostAndPort;

public class AppendLogConfig implements Serializable {
  private static final long serialVersionUID = 1L;
  
//    public HostAndPort redisEndpoint;
//    public HostAndPort protobufEndpoint;

    public int httpPort;

    public GossipConfig gossip;

    public ClusterConfig seedConfig;
    
    public AppendLogConfig deepCopy() {
        return DeepCopy.deepCopy(this);
    }

}