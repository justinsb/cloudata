package com.cloudata.mq.web;

import java.net.URI;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.cloudata.mq.MqModel.Message;
import com.cloudata.mq.MqModel.Queue;
import com.google.common.base.Splitter;
import com.google.protobuf.ByteString;

public class SendMessageAction extends SqsAction {

  @Override
  public void run(HttpServletRequest req, HttpServletResponse resp) throws Exception {
    String queueUrl = getRequiredParameter(req, "QueueUrl");
    String messageBody = getRequiredParameter(req, "MessageBody");

    URI uri = URI.create(queueUrl);
    List<String> tokens = Splitter.on("/").omitEmptyStrings().splitToList(uri.getPath());
    if (tokens.size() != 2) {
      throw new IllegalArgumentException();
    }

    String scope = tokens.get(0);
    String queueName = tokens.get(1);

    QueueUser user = getQueueUser();
    Queue queue = queueService.findQueue(user, scope, queueName);
    if (queue == null) {
      throw new IllegalArgumentException();
    }

    Message.Builder message = Message.newBuilder();

    message.setBody(ByteString.copyFromUtf8(messageBody));

    Message posted = queueService.postMessage(queue, message);

    try (SqsResponseWriter responseWriter = sqsResponseWriterFactory.get(resp)) {
      responseWriter.writeSendMessageResponse(posted);
    }
  }

}
