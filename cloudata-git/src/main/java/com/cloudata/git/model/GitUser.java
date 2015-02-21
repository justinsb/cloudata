package com.cloudata.git.model;

import java.io.IOException;

import org.eclipse.jgit.lib.Repository;

import com.cloudata.auth.AuthenticatedUser;
import com.cloudata.git.config.RepoConfig;
import com.cloudata.git.services.GitRepositoryStore;
import com.google.common.base.Strings;

public abstract class GitUser implements AuthenticatedUser {

  @Override
  public abstract String getId();

  public abstract boolean canAccess(GitRepository repo) throws IOException;

  // public abstract String mapToAbsolutePath(String name);

  // public abstract ObjectStorePath buildObjectStorePath(String absolutePath);

  public static GitUser toGitUser(final GitRepositoryStore gitRepositoryStore, final AuthenticatedUser authenticatedUser) {
    if (authenticatedUser == null) {
      return null;
    }

    return new GitUser() {

      @Override
      public boolean canAccess(GitRepository repo) throws IOException {
        boolean mustExist = true;
        Repository repository = gitRepositoryStore.openRepository(repo, mustExist);
        if (repository == null) {
          return false;
        }
        RepoConfig repoConfig = RepoConfig.build(repo, repository);
        return repoConfig.canAccess(authenticatedUser);
      }

      @Override
      public String getId() {
        return authenticatedUser.getId();
      }

    };
  }
  
}
