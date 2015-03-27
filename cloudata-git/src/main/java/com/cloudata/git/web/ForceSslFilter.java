package com.cloudata.git.web;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.config.ProgramMode;

public class ForceSslFilter implements Filter {
  private static final Logger log = LoggerFactory.getLogger(ForceSslFilter.class);

  final boolean development;

  final String stsHeader;

  public final static String DEFAULT_STS = "Strict-Transport-Security: max-age=10886400";
  public final static String PRELOAD_STS = "Strict-Transport-Security: max-age=10886400; includeSubDomains; preload";

  public ForceSslFilter(String stsHeader) {
    this.stsHeader = stsHeader;
    this.development = ProgramMode.isDevelopment();
  }

  @Override
  public void destroy() {

  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain next) throws IOException,
      ServletException {
    HttpServletRequest httpServletRequest = (HttpServletRequest) request;
    HttpServletResponse httpServletResponse = (HttpServletResponse) response;

    if (development) {
      next.doFilter(request, response);
      return;
    }

    String scheme = httpServletRequest.getScheme();

    String forwardedScheme = httpServletRequest.getHeader("X-Forwarded-Proto");
    if (forwardedScheme != null) {
      scheme = forwardedScheme;
    }

    if ("https".equals(scheme)) {
      httpServletResponse.setHeader("Strict-Transport-Security", stsHeader);

      next.doFilter(request, response);
    } else if ("http".equals(scheme)) {
      try {
        URI requestUri = new URI(httpServletRequest.getRequestURL().toString());

        URI redirectUri = new URI("https", requestUri.getUserInfo(), requestUri.getHost(),
        /* port: */-1, requestUri.getPath(), httpServletRequest.getQueryString(),
        /* fragment: */null);

        httpServletResponse.sendRedirect(redirectUri.toString());
      } catch (URISyntaxException e) {
        throw new IllegalStateException("Error constructing redirect URI", e);
      }
    } else {
      log.warn("Unknown scheme {}", scheme);
      next.doFilter(request, response);
    }
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

}
