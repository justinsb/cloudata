package com.cloudata.mq.web;

import com.cloudata.mq.MqModel.Message;
import com.cloudata.mq.MqModel.Queue;
import com.google.protobuf.ByteString;

public class SendMessageAction extends SqsAction {

  @Override
  public void run() throws Exception {
    String messageBody = getRequiredParameter("MessageBody");

    QueueUser user = getQueueUser();
    Queue queue = getQueue(user);

    Message.Builder message = Message.newBuilder();

    message.setBody(ByteString.copyFromUtf8(messageBody));

    Message posted = queueService.postMessage(queue, message);

    try (SqsResponseWriter responseWriter = buildResponseWriter()) {
      responseWriter.writeSendMessageResponse(posted);
    }
  }

}
