package com.cloudata.mq.web;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.cloudata.datastore.UniqueIndexViolation;
import com.cloudata.mq.MqModel.Queue;

public class CreateQueueAction extends SqsAction {

  @Override
  public void run(HttpServletRequest req, HttpServletResponse resp) throws Exception {
    String queueName = getRequiredParameter(req, "QueueName");

    QueueUser user = getQueueUser();
    Queue.Builder queueBuilder = Queue.newBuilder();
    queueBuilder.setName(queueName);
    Queue queue;
    try {
      queue = queueService.createQueue(user, queueBuilder);
    } catch (UniqueIndexViolation e) {
      throw new AwsException(400, "QueueNameExists");
    }
    try (SqsResponseWriter responseWriter = sqsResponseWriterFactory.get(resp)) {
      responseWriter.writeCreateQueueResponse(queue);
    }
  }

}
