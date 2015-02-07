package com.cloudata.git.web;

import java.util.HashMap;
import java.util.Map;

import com.cloudata.auth.ProjectBasicAuthFilter;
import com.google.gerrit.httpd.GitOverHttpServlet;
import com.google.inject.servlet.ServletModule;

public class WebModule extends ServletModule {
  @Override
  protected void configureServlets() {
    bind(GitOverHttpServlet.class);

//    filter("/*").through(ServletErrorFilter.class);
    filter("/*").through(ProjectBasicAuthFilter.class);

    Map<String, String> params = new HashMap<String, String>();
    serve("/*").with(GitOverHttpServlet.class, params);
  }
}
