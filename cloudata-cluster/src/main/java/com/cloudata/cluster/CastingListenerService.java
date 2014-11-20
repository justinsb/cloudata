package com.cloudata.cluster;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.cluster.ClusterProtocol.BroadcastMessage;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

public class CastingListenerService extends AbstractExecutionThreadService {

  private static final Logger log = LoggerFactory.getLogger(CastingListenerService.class);

  private final DatagramSocket receiveSocket;
  private final ClusterState clusterState;

  protected CastingListenerService(ClusterState clusterState, DatagramSocket receiveSocket) {
    this.receiveSocket = receiveSocket;
    this.clusterState = clusterState;
  }

  private void gotMessage(InetAddress src, BroadcastMessage message) {
    clusterState.notifyBroadcast(src, message);
  }

  @Override
  protected void run() throws Exception {
    byte[] buffer = new byte[12000];

    receiveSocket.setSoTimeout(1000);

    while (isRunning()) {
      DatagramPacket packet = new DatagramPacket(buffer, 0, buffer.length);
      boolean receieved = false;
      try {
        receiveSocket.receive(packet);
        receieved = true;
      } catch (SocketTimeoutException e) {
        // Ignore
      } catch (IOException e) {
        log.warn("Error reading from socket", e);
      }
      if (receieved) {
        BroadcastMessage message = null;

        if (packet.getLength() == buffer.length) {
          log.warn("Message likely truncated; won't try to decode");
        } else {
          try {
            message = BroadcastMessage.parseFrom(new ByteArrayInputStream(buffer, 0, packet.getLength()));
          } catch (IOException e) {
            log.warn("Error parsing broadcast message", e);
          }
        }

        gotMessage(packet.getAddress(), message);
      }
    }
  }

}
