package com.cloudata.mq.web;

import com.cloudata.auth.Project;
import com.cloudata.datastore.UniqueIndexViolation;
import com.cloudata.mq.MqModel.Queue;

public class CreateQueueAction extends SqsAction {

  @Override
  public void run() throws Exception {
    String queueName = getRequiredParameter("QueueName");

    Project project = getProject();
    Queue.Builder queueBuilder = Queue.newBuilder();
    queueBuilder.setName(queueName);
    Queue queue;
    try {
      queue = queueService.createQueue(project, queueBuilder);
    } catch (UniqueIndexViolation e) {
      throw new AwsException(400, "QueueNameExists");
    }
    try (SqsResponseWriter responseWriter = buildResponseWriter()) {
      responseWriter.writeCreateQueueResponse(queue);
    }
  }

}
