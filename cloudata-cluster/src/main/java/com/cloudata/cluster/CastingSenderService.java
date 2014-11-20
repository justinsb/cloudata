package com.cloudata.cluster;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.robotninjas.barge.RaftService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.cluster.ClusterProtocol.BroadcastMessage;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

public class CastingSenderService extends AbstractScheduledService {

  private static final Logger log = LoggerFactory.getLogger(CastingSenderService.class);

  private final ScheduledExecutorService executor;
  private final List<InetSocketAddress> broadcastAddresses;
  private final DatagramSocket sendSocket;

  private BroadcastMessage message;

  protected CastingSenderService(ScheduledExecutorService executor, DatagramSocket sendSocket,
      List<InetSocketAddress> broadcastAddresses) {
    this.sendSocket = sendSocket;
    this.broadcastAddresses = broadcastAddresses;
    this.executor = executor;
  }

  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, 5, TimeUnit.SECONDS);
  }

  @Override
  protected ScheduledExecutorService executor() {
    return executor;
  }

  @Override
  protected void runOneIteration() throws Exception {
    if (message == null) {
      return;
    }

    byte[] data = message.toByteArray();
    for (InetSocketAddress broadcastAddress : broadcastAddresses) {
      DatagramPacket p = new DatagramPacket(data, data.length, broadcastAddress.getAddress(),
          broadcastAddress.getPort());
      try {
        sendSocket.send(p);
        log.debug("Sent broadcast: to {} {}", broadcastAddress, message);
      } catch (IOException e) {
        log.warn("Error sending broadcast", e);
      }
    }
  }

  public BroadcastMessage getMessage() {
    return message;
  }

  public void setMessage(BroadcastMessage message) {
    this.message = message;
  }

}
