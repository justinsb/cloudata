package com.cloudata.git.ddp;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.cloudata.auth.AuthenticatedUser;
import com.cloudata.git.Escaping;
import com.cloudata.git.model.GitRepository;
import com.cloudata.git.model.GitUser;
import com.cloudata.git.services.GitRepositoryStore;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.justinsb.ddpserver.DdpSession;
import com.justinsb.ddpserver.DdpSubscription;
import com.justinsb.ddpserver.Jsonable;
import com.justinsb.ddpserver.triggeredpoll.DdpMethod;
import com.justinsb.ddpserver.triggeredpoll.DdpMethodContext;
import com.justinsb.ddpserver.triggeredpoll.DdpPublish;
import com.justinsb.ddpserver.triggeredpoll.DdpPublishContext;
import com.justinsb.ddpserver.triggeredpoll.SimpleDdpDataSource;
import com.justinsb.ddpserver.triggeredpoll.SimpleDdpSubscription;
import com.justinsb.ddpserver.triggeredpoll.TriggerDdpDataSource;

@Singleton
public class GitDdpDataSource extends SimpleDdpDataSource {
  @Inject
  GitRepositoryStore store;

  public GitDdpDataSource() {
    addPublish("repos", new PublishRepos());
  }

  public class PublishRepos implements DdpPublish {

    @Override
    public DdpSubscription subscribe(DdpPublishContext context, JsonArray params) throws IOException {
      GitUser user = getGitUser(context);
      if (user == null) {
        // XXX: Public repos?
        throw new SecurityException();
      }
      List<GitRepository> repos = store.listRepos(user);

      SimpleDdpSubscription subscription = new SimpleDdpSubscription(context) {

        @Override
        protected Iterable<Entry<String, Jsonable>> getInitialItems() {
          Map<String, Jsonable> items = Maps.newHashMap();

          for (GitRepository repo : repos) {
            String id = Escaping.asBase64Url(repo.getData().getRepositoryId());
            items.put(id, repo);
          }
          return items.entrySet();
        }
      };

      return subscription;
    }
  }

  public GitUser getGitUser(DdpPublishContext context) {
    AuthenticatedUser authenticatedUser = new AuthenticatedUser() {

      @Override
      public String getName() {
        return "justinsb";
      }

      @Override
      public ByteString getUserId() {
        return ByteString.copyFromUtf8("justinsb");
      }
    };
    return store.toGitUser(authenticatedUser);
  }
}
