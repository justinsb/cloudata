package com.cloudata.git;

import java.io.File;
import java.util.EnumSet;

import javax.servlet.DispatcherType;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.config.Configuration;
import com.cloudata.git.web.WebModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;

public class GitServer {
  private static final Logger log = LoggerFactory.getLogger(GitServer.class);

  final File baseDir;
  final int httpPort;

  private Server jetty;

  public GitServer(File baseDir, int httpPort) {
    this.baseDir = baseDir;
    this.httpPort = httpPort;
  }

  public synchronized void start() throws Exception {
    if (jetty != null) {
      throw new IllegalStateException();
    }

    Configuration configuration = Configuration.build();

    Injector injector = Guice.createInjector(new GitModule(configuration), new WebModule());

    // ResourceConfig rc = new PackagesResourceConfig(WebModule.class.getPackage().getName());
    // IoCComponentProviderFactory ioc = new GuiceComponentProviderFactory(rc, injector);
    //
    // this.selector = GrizzlyServerFactory.create(baseUri, rc, ioc);

    this.jetty = new Server(httpPort);

    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");

    FilterHolder filterHolder = new FilterHolder(injector.getInstance(GuiceFilter.class));
    context.addFilter(filterHolder, "*", EnumSet.of(DispatcherType.REQUEST));

    jetty.setHandler(context);

    jetty.start();

  }

  public String getHttpUrl() {
    return "http://localhost:" + httpPort + "/";
  }

  public static void main(String... args) throws Exception {
    File baseDir = new File("gitdata");
    int httpPort = 8888;

    final GitServer server = new GitServer(baseDir, httpPort);
    server.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          server.stop();
        } catch (Exception e) {
          log.warn("Error shutting down HTTP server", e);
        }
      }
    });
  }

  public synchronized void stop() throws Exception {
    if (jetty != null) {
      jetty.stop();
      jetty = null;
    }
  }

}
