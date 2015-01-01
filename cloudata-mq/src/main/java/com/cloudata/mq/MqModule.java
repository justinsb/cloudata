package com.cloudata.mq;

import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.inmem.InMemoryDataStore;
import com.cloudata.mq.web.QueueService;
import com.google.inject.AbstractModule;

public class MqModule extends AbstractModule {

  @Override
  protected void configure() {
    String baseUrl = "http://127.0.0.1:8888/";
    QueueService queueService = new QueueService(baseUrl);
    requestInjection(queueService);
    bind(QueueService.class).toInstance(queueService);

    InMemoryDataStore dataStore = new InMemoryDataStore();
    bind(DataStore.class).toInstance(dataStore);
  }

}
