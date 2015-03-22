package com.cloudata.git.ddp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;

import com.cloudata.auth.AuthenticatedUser;
import com.cloudata.git.Escaping;
import com.cloudata.git.model.GitRepository;
import com.cloudata.git.model.GitUser;
import com.cloudata.git.services.GitRepositoryStore;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.justinsb.ddpserver.DdpJson;
import com.justinsb.ddpserver.DdpPublish;
import com.justinsb.ddpserver.DdpPublishContext;
import com.justinsb.ddpserver.DdpSession;
import com.justinsb.ddpserver.DdpSubscription;
import com.justinsb.ddpserver.Jsonable;
import com.justinsb.ddpserver.triggeredpoll.SimpleDdpDataSource;
import com.justinsb.ddpserver.triggeredpoll.SimpleDdpSubscription;

@Singleton
public class GitDdpDataSource extends SimpleDdpDataSource {
  @Inject
  GitRepositoryStore store;

  @Inject
  DdpMethodLogin ddpMethodLogin;
  @Inject
  DdpMethodLogout ddpMethodLogout;

  public void init() {
    addPublish("repos", new PublishRepos());
    addPublish("revlogs", new PublishGitLog());
    addPublish("diffs", new PublishDiff());
    addPublish("files", new PublishTree());
    addPublish("filecontents", new PublishFileContent());
    addPublish("meteor_autoupdate_clientVersions", new PublishClientVersions());
    addPublish("meteor.loginServiceConfiguration", new MeteorLoginServiceConfiguration());

    addMethod("login", ddpMethodLogin);
    addMethod("logout", ddpMethodLogout);
  }

  public class PublishRepos implements DdpPublish {

    @Override
    public DdpSubscription subscribe(DdpPublishContext context, String name, JsonArray params) throws IOException {

      SimpleDdpSubscription subscription = new SimpleDdpSubscription(context) {
        @Override
        protected Iterable<Entry<String, Jsonable>> getInitialItems() throws IOException {
          Map<String, Jsonable> items = Maps.newHashMap();

          GitUser user = getGitUser(context);

          List<GitRepository> repos;
          if (user != null) {
            repos = store.listRepos(user);
          } else {
            // XXX: Public repos?
            repos = Lists.newArrayList();
          }

          for (GitRepository repo : repos) {
            String id = buildIdForRepo(repo);
            items.put(id, repo);
          }
          return items.entrySet();
        }

      };

      return subscription;
    }
  }

  private String buildIdForRepo(GitRepository repo) {
    return Escaping.asBase64Url(repo.getData().getRepositoryId());
  }

  public class PublishGitLog implements DdpPublish {

    @Override
    public DdpSubscription subscribe(DdpPublishContext context, String name, JsonArray params) throws IOException {

      SimpleDdpSubscription subscription = new SimpleDdpSubscription(context) {
        @Override
        protected Iterable<Entry<String, Jsonable>> getInitialItems() throws Exception {
          Map<String, Jsonable> items = Maps.newHashMap();

          String repoName = DdpJson.requiredString(params, 0);

          GitUser user = getGitUser(context);

          GitRepository gitRepo = store.findRepo(repoName);
          if (gitRepo == null) {
            throw new IllegalArgumentException();
          }
          if (!user.canAccess(gitRepo)) {
            // log.debug("Attempt to access unauthorized repo {}", repo);
            throw new IllegalArgumentException();
          }
          String repoId = buildIdForRepo(gitRepo);

          Repository repository = store.openRepository(user, gitRepo, true);
          Iterable<RevCommit> logs = new Git(repository).log().call();
          for (RevCommit rev : logs) {
            String id = repoId + "_" + rev.getId().name();
            items.put(id, new GitCommit(rev));
          }
          repository.close();
          return items.entrySet();

        }
      };

      return subscription;
    }
  }

  public class PublishTree implements DdpPublish {

    @Override
    public DdpSubscription subscribe(DdpPublishContext context, String name, JsonArray params) throws IOException {

      SimpleDdpSubscription subscription = new SimpleDdpSubscription(context) {
        @Override
        protected Iterable<Entry<String, Jsonable>> getInitialItems() throws Exception {
          Map<String, Jsonable> items = Maps.newHashMap();

          String repoName = DdpJson.requiredString(params, 0);

          GitUser user = getGitUser(context);

          GitRepository gitRepo = store.findRepo(repoName);
          if (gitRepo == null) {
            throw new IllegalArgumentException();
          }
          if (!user.canAccess(gitRepo)) {
            // log.debug("Attempt to access unauthorized repo {}", repo);
            throw new IllegalArgumentException();
          }
          String repoId = buildIdForRepo(gitRepo);

          Repository repository = store.openRepository(user, gitRepo, true);
          Ref head = repository.getRef("HEAD");

          // a RevWalk allows to walk over commits based on some filtering that is
          // defined
          RevWalk walk = new RevWalk(repository);

          RevCommit commit = walk.parseCommit(head.getObjectId());
          RevTree tree = commit.getTree();

          // now use a TreeWalk to iterate over all files in the Tree recursively
          // you can set Filters to narrow down the results if needed
          TreeWalk treeWalk = new TreeWalk(repository);
          treeWalk.addTree(tree);
          treeWalk.setRecursive(true);
          while (treeWalk.next()) {
            String path = treeWalk.getPathString();

            String id = repoId + "_" + path;
            JsonObject json = new JsonObject();
            json.addProperty("repo", repoName);
            json.addProperty("path", path);
            items.put(id, Jsonable.fromJson(json));
          }
          repository.close();
          return items.entrySet();

        }
      };

      return subscription;
    }
  }

  public class PublishFileContent implements DdpPublish {

    @Override
    public DdpSubscription subscribe(DdpPublishContext context, String name, JsonArray params) throws IOException {

      SimpleDdpSubscription subscription = new SimpleDdpSubscription(context) {
        @Override
        protected Iterable<Entry<String, Jsonable>> getInitialItems() throws Exception {
          Map<String, Jsonable> items = Maps.newHashMap();

          String repoName = DdpJson.requiredString(params, 0);
          String path = DdpJson.requiredString(params, 1);

          GitUser user = getGitUser(context);

          GitRepository gitRepo = store.findRepo(repoName);
          if (gitRepo == null) {
            throw new IllegalArgumentException();
          }
          if (!user.canAccess(gitRepo)) {
            // log.debug("Attempt to access unauthorized repo {}", repo);
            throw new IllegalArgumentException();
          }
          String repoId = buildIdForRepo(gitRepo);

          Repository repository = store.openRepository(user, gitRepo, true);
          Ref head = repository.getRef("HEAD");

          // a RevWalk allows to walk over commits based on some filtering that is
          // defined
          RevWalk walk = new RevWalk(repository);

          RevCommit commit = walk.parseCommit(head.getObjectId());
          RevTree tree = commit.getTree();

          // now try to find a specific file
          TreeWalk treeWalk = new TreeWalk(repository);
          treeWalk.addTree(tree);
          treeWalk.setRecursive(true);
          treeWalk.setFilter(PathFilter.create(path));

          while (treeWalk.next()) {
            String treeWalkPath = treeWalk.getPathString();

            ObjectId objectId = treeWalk.getObjectId(0);
            ObjectLoader loader = repository.open(objectId);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            loader.copyTo(baos);

            String contents = new String(baos.toByteArray(), Charsets.UTF_8);

            String id = repoId + "_" + treeWalkPath;
            JsonObject json = new JsonObject();
            json.addProperty("repo", repoName);
            json.addProperty("path", treeWalkPath);
            json.addProperty("contents", contents);
            items.put(id, Jsonable.fromJson(json));
          }

          repository.close();
          return items.entrySet();

        }
      };

      return subscription;
    }
  }

  public class PublishDiff implements DdpPublish {

    @Override
    public DdpSubscription subscribe(DdpPublishContext context, String name, JsonArray params) throws IOException {

      SimpleDdpSubscription subscription = new SimpleDdpSubscription(context) {
        @Override
        protected Iterable<Entry<String, Jsonable>> getInitialItems() throws Exception {
          Map<String, Jsonable> items = Maps.newHashMap();

          String repoName = DdpJson.optionalString(params, 0);
          String sha = DdpJson.optionalString(params, 1);

          GitUser user = getGitUser(context);
          GitRepository gitRepo = store.findRepo(repoName);
          if (gitRepo == null) {
            throw new IllegalArgumentException();
          }
          if (!user.canAccess(gitRepo)) {
            // log.debug("Attempt to access unauthorized repo {}", repo);
            throw new IllegalArgumentException();
          }

          Repository repository = store.openRepository(user, gitRepo, true);

          String repoId = buildIdForRepo(gitRepo);

          // The {tree} will return the underlying tree-id instead of the commit-id itself!
          // For a description of what the carets do see e.g. http://www.paulboxley.com/blog/2011/06/git-caret-and-tilde
          // This means we are selecting the parent of the parent of the parent of the parent of current HEAD and
          // take the tree-ish of it

          ObjectId head = repository.resolve(sha + "^{tree}");
          ObjectId oldHead = repository.resolve(sha + "^^{tree}");

          System.out.println("Printing diff between tree: " + oldHead + " and " + head);

          // prepare the two iterators to compute the diff between
          ObjectReader reader = repository.newObjectReader();
          CanonicalTreeParser oldTreeIter = new CanonicalTreeParser();
          oldTreeIter.reset(reader, oldHead);
          CanonicalTreeParser newTreeIter = new CanonicalTreeParser();
          newTreeIter.reset(reader, head);

          // finally get the list of changed files
          List<DiffEntry> diffs = new Git(repository).diff().setNewTree(newTreeIter).setOldTree(oldTreeIter).call();
          int i = 0;
          for (DiffEntry entry : diffs) {
            String id = repoId + "_" + sha + "_" + i++;

            JsonObject json = new JsonObject();
            json.addProperty("repo", gitRepo.getName());
            json.addProperty("sha", sha);
            json.addProperty("newPath", entry.getNewPath());
            json.addProperty("oldPath", entry.getOldPath());

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DiffFormatter formatter = new DiffFormatter(baos);
            formatter.setRepository(repository);
            formatter.format(entry);
            json.addProperty("diff", new String(baos.toByteArray(), Charsets.UTF_8));

            items.put(id, Jsonable.fromJson(json));
          }

          repository.close();
          return items.entrySet();
        }
      };

      return subscription;
    }
  }

  public GitUser getGitUser(DdpPublishContext context) {
    DdpSession session = context.getSession();

    AuthenticatedUser authenticatedUser = session.getState(AuthenticatedUser.class);
    if (authenticatedUser == null) {
      return null;
    }

    return store.toGitUser(authenticatedUser);
  }
}
