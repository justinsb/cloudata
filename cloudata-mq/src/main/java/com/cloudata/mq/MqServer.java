package com.cloudata.mq;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.mq.web.WebModule;
import com.cloudata.services.CompoundService;
import com.cloudata.services.JettyService;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class MqServer extends CompoundService {
  private static final Logger log = LoggerFactory.getLogger(MqServer.class);
  final int httpPort;

  public MqServer(int httpPort) {
    this.httpPort = httpPort;

  }

  public static void main(String... args) throws Exception {
    int httpPort = 8888;
    final MqServer server = new MqServer(httpPort);
    server.startAsync().awaitRunning();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          server.stopAsync().awaitTerminated();
        } catch (Exception e) {
          log.error("Error stopping server", e);
        }
      }
    });
  }

  @Override
  protected List<com.google.common.util.concurrent.Service> buildServices() {
    Injector injector = Guice.createInjector(new MqModule(), new WebModule());

    List<com.google.common.util.concurrent.Service> services = Lists.newArrayList();

    JettyService jetty = injector.getInstance(JettyService.class);
    jetty.init(httpPort);
    services.add(jetty);

    return services;
  }

}
