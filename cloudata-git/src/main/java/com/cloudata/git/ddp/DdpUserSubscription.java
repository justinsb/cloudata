package com.cloudata.git.ddp;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import com.cloudata.auth.AuthenticatedUser;
import com.cloudata.git.Escaping;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.justinsb.ddpserver.DdpPublishContext;
import com.justinsb.ddpserver.DdpSubscription;
import com.justinsb.ddpserver.Jsonable;
import com.justinsb.ddpserver.triggeredpoll.SimpleDdpSubscription;

public class DdpUserSubscription extends SimpleDdpSubscription {
  AuthenticatedUser user;
  final ByteString userId;

  public DdpUserSubscription(DdpPublishContext context, AuthenticatedUser user) {
    super(context);
    this.user = user;
    this.userId = user.getUserId();
  }

  @Override
  protected Iterable<Entry<String, Jsonable>> getInitialItems() {
    Map<String, Jsonable> items = Maps.newHashMap();

    String userId = Escaping.asBase64Url(user.getUserId());

    JsonObject userFields = new JsonObject();
    userFields.addProperty("username", user.getName());

    items.put(userId, Jsonable.fromJson(userFields));
    return items.entrySet();
  }

}
