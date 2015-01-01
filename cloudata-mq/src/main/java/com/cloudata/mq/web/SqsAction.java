package com.cloudata.mq.web;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import javax.inject.Inject;
import javax.xml.stream.XMLStreamException;

import com.cloudata.datastore.DataStoreException;
import com.cloudata.mq.MqModel.Queue;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

public abstract class SqsAction extends Action {
  @Inject
  protected QueueService queueService;

  @Inject
  private SqsResponseWriter.Factory sqsResponseWriterFactory;

  protected SqsResponseWriter buildResponseWriter() throws XMLStreamException, IOException {
    return sqsResponseWriterFactory.get(response);
  }

  protected QueueUser getQueueUser() {
    String scope = "test";
    return new QueueUser(scope);
  }

  protected Queue getQueue(QueueUser user) throws AwsException, DataStoreException {
    String queueUrl = getRequiredParameter("QueueUrl");
    URI uri = URI.create(queueUrl);
    List<String> tokens = Splitter.on("/").omitEmptyStrings().splitToList(uri.getPath());
    if (tokens.size() != 2) {
      throw new IllegalArgumentException();
    }

    String scope = tokens.get(0);
    String queueName = tokens.get(1);

    Queue queue = queueService.findQueue(user, scope, queueName);
    if (queue == null) {
      throw new IllegalArgumentException();
    }
    return queue;
  }

  protected String getRequiredParameter(String key) throws AwsException {
    String value = request.getParameter(key);
    if (Strings.isNullOrEmpty(value)) {
      throw new AwsException(400, "MissingParameter");
    }
    return value;
  }

  protected int getParameter(String key, int defaultValue) {
    String value = request.getParameter(key);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    return Integer.parseInt(value);
  }

  protected String getParameter(String key, String defaultValue) {
    String value = request.getParameter(key);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    return value;
  }
}
