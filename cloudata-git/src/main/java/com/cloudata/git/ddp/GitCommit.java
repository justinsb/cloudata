package com.cloudata.git.ddp;

import org.eclipse.jgit.revwalk.RevCommit;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.justinsb.ddpserver.Jsonable;

public class GitCommit implements Jsonable {

  final RevCommit revCommit;

  public GitCommit(RevCommit revCommit) {
    this.revCommit = revCommit;
  }

  @Override
  public JsonElement toJsonElement() {
    JsonObject json = new JsonObject();
    json.addProperty("shortMessage", revCommit.getShortMessage());
    json.addProperty("fullMessage", revCommit.getFullMessage());
    json.addProperty("sha", revCommit.getId().name());
    return json;
  }

}
