package com.cloudata.mq.web;

import com.google.inject.servlet.ServletModule;

public class WebModule extends ServletModule {
  @Override
  protected void configureServlets() {
    ActionFactory sqsActionFactory = new ActionFactory();
    sqsActionFactory.add("ListQueues", ListQueueAction.class);
    sqsActionFactory.add("CreateQueue", CreateQueueAction.class);
    sqsActionFactory.add("SendMessage", SendMessageAction.class);

    requestInjection(sqsActionFactory);
    bind(ActionFactory.class).toInstance(sqsActionFactory);

    serve("/*").with(SqsServlet.class);
  }
}
