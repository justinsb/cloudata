package com.cloudata.git.model;

import com.cloudata.git.Escaping;
import com.cloudata.git.GitModel.RepositoryData;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.justinsb.ddpserver.Jsonable;

public class GitRepository implements Jsonable {

  final String objectPath;
  final RepositoryData data;

  public GitRepository(String objectPath, RepositoryData data) {
    this.objectPath = objectPath;
    this.data = data;
  }

  public boolean isPublicRead() {
    return false;
  }

  public String getObjectPath() {
    return objectPath;
  }

  public RepositoryData getData() {
    return data;
  }

  @Override
  public JsonElement toJsonElement() {
    // XXX: Use protobuf -> JSON encoder
    JsonObject json = new JsonObject();
    // json.addProperty("_id", Escaping.asBase64Url(data.getRepositoryId()));
    json.addProperty("name", data.getName());
    // json.addProperty("owner", data.getOwner());
    return json;
  }

  public String getName() {
    return data.getName();
  }

}
