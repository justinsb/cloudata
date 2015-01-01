package com.cloudata.mq.web;

import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet to support the AWS SQS API
 *
 * We use a servlet because the AWS APIs aren't really RESTful
 *
 */
@Singleton
public class SqsServlet extends HttpServlet {
  private static final Logger log = LoggerFactory.getLogger(SqsServlet.class);

  private static final long serialVersionUID = 1L;

  @Inject
  ActionFactory actionFactory;

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    String actionName = request.getParameter("Action");
    log.debug("Got request: {}", actionName);
    Action action = actionFactory.getAction(actionName);
    if (action == null) {
      log.debug("InvalidAction: {}", actionName);
      sendError(response, 400, "InvalidAction");
      return;
    }

    try {
      action.init(request, response);

      action.run();
    } catch (Exception e) {
      log.error("Unexpected failure running action", e);
      sendError(response, 500, "InternalFailure");
    }
  }

  private void sendError(HttpServletResponse resp, int code, String message) throws IOException {
    resp.sendError(code, message);
  }

}
