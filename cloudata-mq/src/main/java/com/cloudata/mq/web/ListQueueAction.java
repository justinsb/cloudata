package com.cloudata.mq.web;

import java.util.List;

import com.cloudata.mq.MqModel.Queue;

public class ListQueueAction extends SqsAction {

  @Override
  public void run() throws Exception {
    String queueNamePrefix = getParameter("QueueNamePrefix", null);
    QueueUser user = getQueueUser();
    List<Queue> queues = queueService.listQueues(user, queueNamePrefix);

    try (SqsResponseWriter responseWriter = buildResponseWriter()) {
      responseWriter.writeListQueuesResponse(queues);
    }
  }

}
