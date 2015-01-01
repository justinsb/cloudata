package com.cloudata.mq.web;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import com.google.common.base.Strings;

public abstract class SqsAction extends Action {
  @Inject
  protected QueueService queueService;

  @Inject
  protected SqsResponseWriter.Factory sqsResponseWriterFactory;

  protected QueueUser getQueueUser() {
    String scope = "test";
    return new QueueUser(scope);
  }

  protected String getRequiredParameter(HttpServletRequest req, String key) throws AwsException {
    String value = req.getParameter(key);
    if (Strings.isNullOrEmpty(value)) {
      throw new AwsException(400, "MissingParameter");
    }
    return value;
  }
}
