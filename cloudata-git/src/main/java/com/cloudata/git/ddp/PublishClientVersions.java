package com.cloudata.git.ddp;

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.justinsb.ddpserver.DdpPublish;
import com.justinsb.ddpserver.DdpPublishContext;
import com.justinsb.ddpserver.DdpSubscription;
import com.justinsb.ddpserver.Jsonable;
import com.justinsb.ddpserver.triggeredpoll.SimpleDdpSubscription;

public class PublishClientVersions implements DdpPublish {

  @Override
  public DdpSubscription subscribe(DdpPublishContext context, JsonArray params) throws Exception {
    SimpleDdpSubscription subscription = new SimpleDdpSubscription(context) {

      @Override
      protected Iterable<Entry<String, Jsonable>> getInitialItems() {
        Map<String, Jsonable> items = Maps.newHashMap();

        return items.entrySet();
      }
    };

    return subscription;
  }

}
