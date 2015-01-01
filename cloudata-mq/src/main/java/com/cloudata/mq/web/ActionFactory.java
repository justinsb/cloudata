package com.cloudata.mq.web;

import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.google.common.collect.Maps;
import com.google.inject.Injector;

@Singleton
public class ActionFactory {
  @Inject
  Injector injector;

  final Map<String, Provider<Action>> actions = Maps.newHashMap();

  public Action getAction(String actionName) {
    if (actionName == null) {
      return null;
    }

    Provider<Action> provider = actions.get(actionName);
    if (provider == null) {
      return null;
    }
    return provider.get();
  }

  public void add(String actionName, final Class<? extends Action> clazz) {
    actions.put(actionName, new Provider<Action>() {

      @Override
      public Action get() {
        return injector.getInstance(clazz);
      }

    });

  }
}
