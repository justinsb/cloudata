package com.cloudata.mq.web;

import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.DataStoreException;
import com.cloudata.datastore.Modifier;
import com.cloudata.datastore.WhereModifier;
import com.cloudata.mq.MqModel.Message;
import com.cloudata.mq.MqModel.Queue;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

@Singleton
public class QueueService {

  @Inject
  DataStore dataStore;

  final String baseUrl;

  public QueueService(String baseUrl) {
    this.baseUrl = baseUrl;
  }

  public List<Queue> listQueues(QueueUser user, String queueNamePrefix) throws DataStoreException {
    Queue.Builder matcher = Queue.newBuilder();
    matcher.setScope(user.getScope());

    List<Queue> ret = Lists.newArrayList();
    for (Queue queue : dataStore.find(matcher.build())) {
      if (queueNamePrefix != null && !queue.getName().startsWith(queueNamePrefix)) {
        continue;
      }
      ret.add(queue);
    }

    return ret;
  }

  public String buildQueueUrl(Queue queue) {
    return "http://" + baseUrl + "/" + queue.getScope() + "/" + queue.getName();
  }

  public Queue createQueue(QueueUser user, Queue.Builder queue) throws DataStoreException {
    if (!queue.hasName()) {
      throw new IllegalArgumentException();
    }
    queue.setScope(user.getScope());
    queue.setQueueId(Randoms.buildId());

    Queue inserted = queue.build();
    dataStore.insert(inserted);

    return inserted;
  }

  public Queue findQueue(QueueUser user, String scope, String queueName) throws DataStoreException {
    if (!user.getScope().equals(scope)) {
      return null;
    }
    Queue.Builder matcher = Queue.newBuilder();
    matcher.setScope(user.getScope());
    matcher.setName(queueName);

    return dataStore.findOne(matcher.build());
  }

  public Message postMessage(Queue queue, Message.Builder message) throws DataStoreException {
    if (!message.hasBody()) {
      throw new IllegalArgumentException();
    }

    message.setQueueId(queue.getQueueId());
    message.setMessageId(Randoms.buildId());
    message.setReceiptHandle(ByteString.EMPTY);

    HashCode hc = Hashing.md5().newHasher().putBytes(message.getBody().toByteArray()).hash();
    message.setMessageBodyMd5(ByteString.copyFrom(hc.asBytes()));

    Message insert = message.build();
    dataStore.insert(insert);
    return insert;
  }

  public List<Message> receiveMessages(Queue queue, int maxNumberOfMessages) throws DataStoreException {
    Message.Builder matcher = Message.newBuilder();
    matcher.setQueueId(queue.getQueueId());
    matcher.setReceiptHandle(ByteString.EMPTY);

    List<Message> received = Lists.newArrayList();

    // TODO: Limit or iterable
    for (Message message : dataStore.find(matcher.build())) {
      ByteString receiptHandle = Randoms.buildId();
      Message.Builder update = Message.newBuilder(message);
      update.setReceiptHandle(receiptHandle);
      Message updated = update.build();
      Modifier whereReceiptHandleEmpty = WhereModifier.create(Message.newBuilder().setReceiptHandle(ByteString.EMPTY)
          .build());
      if (dataStore.update(updated, whereReceiptHandleEmpty)) {
        received.add(updated);
        if (received.size() >= maxNumberOfMessages) {
          break;
        }
      }
    }

    return received;
  }
}
