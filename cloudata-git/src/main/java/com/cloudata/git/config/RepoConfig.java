package com.cloudata.git.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Map;

import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.auth.AuthenticatedUser;
import com.cloudata.git.GitModel.RepositoryData;
import com.cloudata.git.model.GitRepository;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;

public class RepoConfig {
  private static final Logger log = LoggerFactory.getLogger(RepoConfig.class);
  final Map<String, RepoUserConfig> users;
  final RepositoryData repositoryData;

  private RepoConfig(RepositoryData repositoryData, Map<String, RepoUserConfig> users) {
    this.repositoryData = repositoryData;
    this.users = users;
  }

  public static RepoConfig build(GitRepository repo, Repository repository) throws IOException {
    ObjectId commitId = repository.resolve("refs/heads/_config");
    Map<String, RepoUserConfig> users = Collections.emptyMap();

    if (commitId != null) {

      RevWalk revWalk = null;
      TreeWalk treeWalk = null;

      try {
        revWalk = new RevWalk(repository);
        RevCommit commit = revWalk.parseCommit(commitId);
        RevTree tree = commit.getTree();

        treeWalk = new TreeWalk(repository);
        treeWalk.addTree(tree);

        users = parseUsers(treeWalk);

      } finally {
        if (treeWalk != null) {
          treeWalk.getObjectReader().release();
          treeWalk.release();
        }
        if (revWalk != null) {
          revWalk.dispose();
        }
      }
    }

    return new RepoConfig(repo.getData(), users);
  }

  private static Map<String, RepoUserConfig> parseUsers(TreeWalk treeWalk) throws IOException {
    treeWalk.setRecursive(true);
    treeWalk.setFilter(PathFilter.create("users/"));
    ObjectReader objectReader = treeWalk.getObjectReader();
    Map<String, RepoUserConfig> users = Maps.newHashMap();

    while (treeWalk.next()) {
      String name = treeWalk.getNameString();

      log.debug("Found entry {}", treeWalk.getPathString());

      ObjectId objectId = treeWalk.getObjectId(0);
      ObjectLoader objectLoader = objectReader.open(objectId);
      try (InputStream is = objectLoader.openStream()) {
        try (InputStreamReader reader = new InputStreamReader(is, Charsets.UTF_8)) {
          users.put(name, RepoUserConfig.parse(name, reader));
        }
      }
    }

    return users;
  }

  public boolean canAccess(AuthenticatedUser authenticatedUser) {
    String userId = authenticatedUser.getId();
    if (userId.equals(repositoryData.getOwner())) {
      return true;
    }
    RepoUserConfig repoUserConfig = users.get(userId);
    if (repoUserConfig != null) {
      return true;
    }
    return false;
  }
}
