package com.cloudata.git;

import java.io.File;
import java.util.EnumSet;

import javax.servlet.DispatcherType;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.auth.ProjectBasicAuthFilter;
import com.cloudata.config.Configuration;
import com.cloudata.config.ProgramMode;
import com.cloudata.git.ddp.GitDdpDataSource;
import com.cloudata.git.web.ForceSslFilter;
import com.google.gerrit.httpd.GitOverHttpFilter;
import com.google.gson.JsonObject;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.justinsb.ddpserver.DdpEndpoints;
import com.justinsb.meteor.BoilerplateFilter;

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

    context.addFilter(new FilterHolder(new ForceSslFilter(ForceSslFilter.DEFAULT_STS)), "/",
        EnumSet.of(DispatcherType.REQUEST));

    GitDdpDataSource ddpDataSource = injector.getInstance(GitDdpDataSource.class);
    ddpDataSource.init();

    DdpEndpoints.register(context, ddpDataSource);

    File meteorBaseDir = new File("meteor/programs/web.browser");
    if (ProgramMode.isDevelopment()) {
      meteorBaseDir = new File("target/meteor/bundle/programs/web.browser");
    }
    log.debug("Base dir is {}", meteorBaseDir);

    JsonObject meteorRuntimeConfig = new JsonObject();
    // e.g.
    // {"meteorRelease":"0.7.1.1","ROOT_URL":"http://someapp.meteor.com","ROOT_URL_PATH_PREFIX":"","accountsConfigCalled":true,"autoupdateVersion":"1d065893-1234-1234-1401-fb41bbeaaa2f"};</script>

    String bundledJsCssPrefix = ""; // "/";
    BoilerplateFilter boilerplate = new BoilerplateFilter(meteorBaseDir, meteorRuntimeConfig, bundledJsCssPrefix);
    context.addFilter(new FilterHolder(boilerplate), "/", EnumSet.of(DispatcherType.REQUEST));

    // FilterHolder filterHolder = new FilterHolder(injector.getInstance(GuiceFilter.class));
    // context.addFilter(filterHolder, "*", EnumSet.of(DispatcherType.REQUEST));

    context.setBaseResource(Resource.newResource(meteorBaseDir));
    context.addServlet(new ServletHolder(DefaultServlet.class), "/*");

    ProjectBasicAuthFilter projectBasicAuthFilter = injector.getInstance(ProjectBasicAuthFilter.class);
    context.addFilter(new FilterHolder(projectBasicAuthFilter), "/*", EnumSet.allOf(DispatcherType.class));

    // GitOverHttpServlet gitOverHttpServlet = injector.getInstance(GitOverHttpServlet.class);
    // context.addServlet(new ServletHolder(gitOverHttpServlet), "/*");
    GitOverHttpFilter gitOverHttpFilter = injector.getInstance(GitOverHttpFilter.class);
    context.addFilter(new FilterHolder(gitOverHttpFilter), "/*", EnumSet.of(DispatcherType.REQUEST));

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
