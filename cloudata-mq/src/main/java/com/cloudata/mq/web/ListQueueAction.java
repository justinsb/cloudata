package com.cloudata.mq.web;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.cloudata.mq.MqModel.Queue;

public class ListQueueAction extends SqsAction {

  @Override
  public void run(HttpServletRequest req, HttpServletResponse resp) throws Exception {
    String queueNamePrefix = req.getParameter("QueueNamePrefix");
    QueueUser user = getQueueUser();
    List<Queue> queues = queueService.listQueues(user, queueNamePrefix);

    try (SqsResponseWriter responseWriter = sqsResponseWriterFactory.get(resp)) {
      responseWriter.writeListQueuesResponse(queues);
    }
  }

}
