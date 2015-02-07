package com.cloudata.mq.web;

import java.util.List;
import java.util.Optional;

import com.cloudata.auth.Project;
import com.cloudata.mq.MqModel.Queue;

public class ListQueueAction extends SqsAction {

  @Override
  public void run() throws Exception {
    Optional<String> queueNamePrefix = getOptionalParameter("QueueNamePrefix");
    Project project = getProject();
    List<Queue> queues = queueService.listQueues(project, queueNamePrefix);

    try (SqsResponseWriter responseWriter = buildResponseWriter()) {
      responseWriter.writeListQueuesResponse(queues);
    }
  }

}
