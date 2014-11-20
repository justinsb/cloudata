package com.cloudata.util;

import java.net.InetSocketAddress;

import com.google.common.net.HostAndPort;

public class InetSocketAddresses {
  public static InetSocketAddress fromHostAndPort(HostAndPort hostAndPort) {
    return new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort());
  }
}
