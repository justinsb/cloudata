package com.cloudata.mq.web;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import com.cloudata.mq.MqModel.Message;
import com.cloudata.mq.MqModel.Queue;
import com.cloudata.mq.MqModel.ReceiptHandle;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;

public class SqsResponseWriter implements Closeable {

  private static final BaseEncoding BASE16_LOWERCASE = BaseEncoding.base16().lowerCase();

  private static final String NS_URI = "http://queue.amazonaws.com/doc/2012-11-05/";

  private XMLStreamWriter writer;
  private QueueService queueService;

  @Singleton
  public static class Factory {
    final XMLOutputFactory factory = XMLOutputFactory.newInstance();

    @Inject
    QueueService queueService;

    public SqsResponseWriter get(HttpServletResponse resp) throws XMLStreamException, IOException {
      XMLStreamWriter writer = factory.createXMLStreamWriter(resp.getOutputStream());

      return new SqsResponseWriter(queueService, writer);
    }

  }

  private SqsResponseWriter(QueueService queueService, XMLStreamWriter writer) {
    this.queueService = queueService;
    this.writer = writer;
  }

  public void writeSendMessageResponse(Message posted) throws XMLStreamException {
    writer.writeStartDocument();

    writer.writeStartElement("SendMessageResponse");
    writer.writeNamespace("", NS_URI);

    writer.writeStartElement("SendMessageResult");
    writeMessage(posted);
    writer.writeEndElement();

    writeResponseMetadata();

    writer.writeEndElement();

    writer.writeEndDocument();
  }

  public void writeReceiveMessageResponse(List<Message> received) throws XMLStreamException {
    writer.writeStartDocument();

    writer.writeStartElement("ReceiveMessageResponse");
    writer.writeNamespace("", NS_URI);

    writer.writeStartElement("ReceiveMessageResult");
    for (Message message : received) {
      writer.writeStartElement("Message");
      writeReceivedMessage(message);
      writer.writeEndElement();
    }
    writer.writeEndElement();

    writeResponseMetadata();

    writer.writeEndElement();

    writer.writeEndDocument();
  }

  public void writeDeleteMessageResponse() throws XMLStreamException {
    writer.writeStartDocument();

    writer.writeStartElement("DeleteMessageResponse");
    writer.writeNamespace("", NS_URI);

    writeResponseMetadata();

    writer.writeEndElement();

    writer.writeEndDocument();
  }

  private void writeMessage(Message message) throws XMLStreamException {

    ByteString messageId = message.getMessageId();
    UUID uuid = UUID.nameUUIDFromBytes(messageId.toByteArray());
    writeElement("MessageId", uuid.toString());

    ByteString messageBodyMd5 = message.getMessageBodyMd5();
    writeElement("MD5OfMessageBody", BASE16_LOWERCASE.encode(messageBodyMd5.toByteArray()));
  }

  private void writeReceivedMessage(Message message) throws XMLStreamException {
    writeElement("Body", message.getBody().toStringUtf8());

    ByteString messageBodyMd5 = message.getMessageBodyMd5();
    writeElement("MD5OfBody", BASE16_LOWERCASE.encode(messageBodyMd5.toByteArray()));

    if (message.hasReceiptHandleNonce()) {
      ReceiptHandle.Builder b = ReceiptHandle.newBuilder();
      b.setMessageId(message.getMessageId());
      b.setNonce(message.getReceiptHandleNonce());

      writeElement("ReceiptHandle", BaseEncoding.base64Url().encode(b.build().toByteArray()));
    }

    ByteString messageId = message.getMessageId();
    UUID uuid = UUID.nameUUIDFromBytes(messageId.toByteArray());
    writeElement("MessageId", uuid.toString());
  }

  public void writeCreateQueueResponse(Queue queue) throws XMLStreamException {
    writer.writeStartDocument();

    writer.writeStartElement("CreateQueueResponse");
    writer.writeNamespace("", NS_URI);

    writer.writeStartElement("CreateQueueResult");

    writeElement("QueueUrl", queueService.buildQueueUrl(queue));

    writer.writeEndElement();

    writeResponseMetadata();

    writer.writeEndElement();

    writer.writeEndDocument();
  }

  public void writeListQueuesResponse(List<Queue> queues) throws XMLStreamException {
    writer.writeStartDocument();

    writer.writeStartElement("ListQueuesResponse");
    writer.writeNamespace("", NS_URI);

    writer.writeStartElement("ListQueuesResult");
    for (Queue queue : queues) {
      writeListQueueResult(queue);
    }
    writer.writeEndElement();

    writeResponseMetadata();

    writer.writeEndElement();

    writer.writeEndDocument();
  }

  private void writeListQueueResult(Queue queue) throws XMLStreamException {
    writeElement("QueueUrl", queueService.buildQueueUrl(queue));
  }

  private void writeResponseMetadata() throws XMLStreamException {
    writer.writeStartElement("ResponseMetadata");

    String requestId = UUID.randomUUID().toString();
    writeElement("RequestId", requestId);

    writer.writeEndElement();
  }

  private void writeElement(String key, String value) throws XMLStreamException {
    writer.writeStartElement(key);
    writer.writeCharacters(value);
    writer.writeEndElement();
  }

  @Override
  public void close() throws IOException {
    try {
      writer.flush();
      writer.close();
    } catch (XMLStreamException e) {
      throw new IOException("Error closing stream", e);
    }
  }

}
