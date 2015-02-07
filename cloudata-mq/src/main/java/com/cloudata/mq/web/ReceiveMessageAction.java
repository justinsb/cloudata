package com.cloudata.mq.web;

import java.util.List;

import com.cloudata.mq.MqModel.Message;
import com.cloudata.mq.MqModel.Queue;

public class ReceiveMessageAction extends SqsAction {

  @Override
  public void run() throws Exception {
    int maxNumberOfMessages = getParameter("MaxNumberOfMessages", 1);

    Queue queue = getQueue();

    List<Message> received = queueService.receiveMessages(queue, maxNumberOfMessages);

    try (SqsResponseWriter responseWriter = buildResponseWriter()) {
      responseWriter.writeReceiveMessageResponse(received);
    }
  }

}
