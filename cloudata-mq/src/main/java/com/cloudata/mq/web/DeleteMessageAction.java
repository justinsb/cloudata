package com.cloudata.mq.web;

import com.cloudata.mq.MqModel.Queue;
import com.cloudata.mq.MqModel.ReceiptHandle;
import com.google.common.io.BaseEncoding;

public class DeleteMessageAction extends SqsAction {

  @Override
  public void run() throws Exception {
    ReceiptHandle receiptHandle = ReceiptHandle.parseFrom(BaseEncoding.base64Url().decode(
        getRequiredParameter("ReceiptHandle")));

    QueueUser user = getQueueUser();
    Queue queue = getQueue(user);

    queueService.deleteMessage(queue, receiptHandle);

    try (SqsResponseWriter responseWriter = buildResponseWriter()) {
      responseWriter.writeDeleteMessageResponse();
    }
  }

}
