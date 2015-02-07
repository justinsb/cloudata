package com.cloudata.mq.web;

import java.util.List;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.cloudata.Randoms;
import com.cloudata.auth.Project;
import com.cloudata.datastore.ComparatorModifier;
import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.DataStoreException;
import com.cloudata.datastore.LimitModifier;
import com.cloudata.datastore.Modifier;
import com.cloudata.datastore.WhereModifier;
import com.cloudata.mq.MqModel.Message;
import com.cloudata.mq.MqModel.Queue;
import com.cloudata.mq.MqModel.ReceiptHandle;
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

  public List<Queue> listQueues(Project project, Optional<String> queueNamePrefix) throws DataStoreException {
    Queue.Builder matcher = Queue.newBuilder();
    matcher.setProjectId(project.getId());

    List<Modifier> modifiers = Lists.newArrayList();
    if (queueNamePrefix.isPresent()) {
      modifiers.add(ComparatorModifier.startsWith(Queue.newBuilder().setName(queueNamePrefix.get()).build()));
    }
    List<Queue> ret = Lists.newArrayList();
    for (Queue queue : dataStore.find(matcher.build(), modifiers)) {
      ret.add(queue);
    }

    return ret;
  }

  public String buildQueueUrl(Queue queue) {
    return "http://" + baseUrl + "/" + queue.getProjectId().toStringUtf8() + "/" + queue.getName();
  }

  public Queue createQueue(Project project, Queue.Builder queue) throws DataStoreException {
    if (!queue.hasName()) {
      throw new IllegalArgumentException();
    }
    queue.setProjectId(project.getId());
    queue.setQueueId(Randoms.buildId());

    Queue inserted = queue.build();
    dataStore.insert(inserted);

    return inserted;
  }

  public Queue findQueue(Project project, String queueName) throws DataStoreException {
    Queue.Builder matcher = Queue.newBuilder();
    matcher.setProjectId(project.getId());
    matcher.setName(queueName);

    return dataStore.findOne(matcher.build());
  }

  public Message postMessage(Queue queue, Message.Builder message) throws DataStoreException {
    if (!message.hasBody()) {
      throw new IllegalArgumentException();
    }

    message.setQueueId(queue.getQueueId());
    message.setMessageId(Randoms.buildId());
    // The message is immediately visible
    message.setInvisibleUntil(now() - 1000);

    message.setVersion(1);

    HashCode hc = Hashing.md5().newHasher().putBytes(message.getBody().toByteArray()).hash();
    message.setMessageBodyMd5(ByteString.copyFrom(hc.asBytes()));

    Message insert = message.build();
    dataStore.insert(insert);
    return insert;
  }

  private static long now() {
    return System.currentTimeMillis();
  }

  public List<Message> receiveMessages(Queue queue, int maxNumberOfMessages) throws DataStoreException {
    Message.Builder matcher = Message.newBuilder();
    matcher.setQueueId(queue.getQueueId());

    List<Modifier> modifiers = Lists.newArrayList();

    // Query for a few more than our limit, in case of simultaneous queries
    int queryLimit = Math.max(maxNumberOfMessages + (maxNumberOfMessages / 2), 4);
    modifiers.add(new LimitModifier(queryLimit));

    long now = now();
    Modifier invisibleUntilFilter = ComparatorModifier.lessThan(Message.newBuilder().setInvisibleUntil(now).build());
    modifiers.add(invisibleUntilFilter);

    List<Message> received = Lists.newArrayList();

    // TODO: Limit or iterable
    for (Message message : dataStore.find(matcher.build(), modifiers)) {
      Message.Builder update = Message.newBuilder(message);
      update.setReceiptHandleNonce(Randoms.buildId());
      update.setVersion(message.getVersion() + 1);

      long visibilityTimeout = 30000;
      update.setInvisibleUntil(now() + visibilityTimeout);

      Message updated = update.build();
      Modifier whereReceiptHandleEmpty = WhereModifier.create(Message.newBuilder().setVersion(message.getVersion())
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

  public boolean deleteMessage(Queue queue, ReceiptHandle receiptHandle) throws DataStoreException {
    Message.Builder matcher = Message.newBuilder();
    matcher.setQueueId(queue.getQueueId());
    matcher.setReceiptHandleNonce(receiptHandle.getNonce());
    matcher.setMessageId(receiptHandle.getMessageId());

    Message message = dataStore.findOne(matcher.build());
    if (message == null) {
      return false;
    }

    Modifier whereReceiptHandleMatches = WhereModifier.create(Message.newBuilder().setVersion(matcher.getVersion())
        .build());
    return dataStore.delete(message, whereReceiptHandleMatches);
  }
}
