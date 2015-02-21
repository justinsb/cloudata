package com.cloudata.git;

import java.io.File;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.DispatcherType;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.auth.ProjectBasicAuthFilter;
import com.cloudata.config.Configuration;
import com.cloudata.git.ddp.GitDdpDataSource;
import com.google.gerrit.httpd.GitOverHttpServlet;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.justinsb.ddpserver.DdpDataSource;
import com.justinsb.ddpserver.DdpEndpoints;
import com.justinsb.ddpserver.DdpMergeBox;
import com.justinsb.ddpserver.DdpSession;
import com.justinsb.ddpserver.DdpSubscription;

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

    Injector injector = Guice.createInjector(new GitModule(configuration)); //, new WebModule());

    // ResourceConfig rc = new PackagesResourceConfig(WebModule.class.getPackage().getName());
    // IoCComponentProviderFactory ioc = new GuiceComponentProviderFactory(rc, injector);
    //
    // this.selector = GrizzlyServerFactory.create(baseUri, rc, ioc);

    Server server = new Server();

    ServerConnector connector = new ServerConnector(server);
    connector.setPort(httpPort);
    server.addConnector(connector);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);

    this.jetty = server;

    GitDdpDataSource ddpDataSource = injector.getInstance(GitDdpDataSource.class);
    ddpDataSource.init();
    
    DdpEndpoints.register(context, ddpDataSource);

    // FilterHolder filterHolder = new FilterHolder(injector.getInstance(GuiceFilter.class));
    // context.addFilter(filterHolder, "*", EnumSet.of(DispatcherType.REQUEST));

    ProjectBasicAuthFilter projectBasicAuthFilter = injector.getInstance(ProjectBasicAuthFilter.class);
    context.addFilter(new FilterHolder(projectBasicAuthFilter), "/*", EnumSet.allOf(DispatcherType.class));

    GitOverHttpServlet gitOverHttpServlet = injector.getInstance(GitOverHttpServlet.class);
    context.addServlet(new ServletHolder(gitOverHttpServlet), "/*");

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
