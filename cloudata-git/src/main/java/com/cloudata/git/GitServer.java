package com.cloudata.git;

import java.io.File;
import java.util.EnumSet;

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
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.justinsb.ddpserver.DdpEndpoints;

public class GitServer {
  private static final Logger log = LoggerFactory.getLogger(GitServer.class);

  private static final int DEFAULT_PORT = 8080;

  final File baseDir;

  private Server jetty;

  final Configuration configuration;

  public GitServer(Configuration configuration, File baseDir) {
    this.configuration = configuration;
    this.baseDir = baseDir;
  }

  public synchronized void start() throws Exception {
    if (jetty != null) {
      throw new IllegalStateException();
    }

    Injector injector = Guice.createInjector(new GitModule(configuration)); // , new WebModule());

    int httpPort = configuration.get("http.port", DEFAULT_PORT);

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

  // public String getHttpUrl() {
  // return "http://localhost:" + httpPort + "/";
  // }

  public static void main(String... args) throws Exception {
    Configuration configuration = Configuration.build();
    File baseDir = new File("gitdata");

    final GitServer server = new GitServer(configuration, baseDir);
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
