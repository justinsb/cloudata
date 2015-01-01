package com.cloudata.mq.web;

import com.cloudata.datastore.UniqueIndexViolation;
import com.cloudata.mq.MqModel.Queue;

public class CreateQueueAction extends SqsAction {

  @Override
  public void run() throws Exception {
    String queueName = getRequiredParameter("QueueName");

    QueueUser user = getQueueUser();
    Queue.Builder queueBuilder = Queue.newBuilder();
    queueBuilder.setName(queueName);
    Queue queue;
    try {
      queue = queueService.createQueue(user, queueBuilder);
    } catch (UniqueIndexViolation e) {
      throw new AwsException(400, "QueueNameExists");
    }
    try (SqsResponseWriter responseWriter = buildResponseWriter()) {
      responseWriter.writeCreateQueueResponse(queue);
    }
  }

}
