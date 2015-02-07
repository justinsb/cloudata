package com.cloudata.services;

import java.util.EnumSet;

import javax.inject.Inject;
import javax.servlet.DispatcherType;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.servlet.GuiceFilter;

public class JettyService extends AbstractService {

  private static final Logger log = LoggerFactory.getLogger(JettyService.class);

  private Server jetty;

  @Inject
  GuiceFilter guiceFilter;

  private int httpPort;

  @Override
  protected void doStart() {
    try {
      Preconditions.checkState(jetty == null);
      Preconditions.checkState(httpPort != 0);

      // ResourceConfig rc = new PackagesResourceConfig(WebModule.class.getPackage().getName());
      // IoCComponentProviderFactory ioc = new GuiceComponentProviderFactory(rc, injector);

      // this.selector = GrizzlyServerFactory.create(baseUri, rc, ioc);

      this.jetty = new Server(httpPort);

      ServletContextHandler context = new ServletContextHandler();
      context.setContextPath("/");
      jetty.setHandler(context);

      configureContext(context);

      jetty.start();
      while (true) {
        String state = jetty.getState();
        if (state.equals(AbstractLifeCycle.STARTED)) {
          break;
        } else if (state.equals(AbstractLifeCycle.STARTING)) {
          log.debug("Waiting for jetty; state={}", state);
          Thread.sleep(500);
        } else {
          throw new IllegalStateException("Jetty reached unexpected state: " + state);
        }
      }

      this.notifyStarted();
    } catch (Exception e) {
      this.notifyFailed(e);
    }

  }

  protected void configureContext(ServletContextHandler context) throws Exception {
    FilterHolder filterHolder = new FilterHolder(guiceFilter);
    context.addFilter(filterHolder, "*", EnumSet.of(DispatcherType.REQUEST));
  }

  @Override
  protected void doStop() {
    try {
      if (jetty != null) {
        log.debug("Stopping jetty");
        jetty.stop();
        while (true) {
          String state = jetty.getState();
          if (state.equals(AbstractLifeCycle.STOPPED)) {
            log.debug("Stopped jetty");
            break;
          } else if (state.equals(AbstractLifeCycle.STOPPING)) {
            log.debug("Waiting for jetty; state={}", state);
            Thread.sleep(500);
          } else {
            throw new IllegalStateException("Jetty reached unexpected state: " + state);
          }
        }

        jetty = null;
      }

      this.notifyStopped();
    } catch (Exception e) {
      this.notifyFailed(e);
    }
  }

  public void init(int httpPort) {
    this.httpPort = httpPort;
  }

}
