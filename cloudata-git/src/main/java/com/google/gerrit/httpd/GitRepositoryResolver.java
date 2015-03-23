package com.google.gerrit.httpd;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.resolver.RepositoryResolver;
import org.eclipse.jgit.transport.resolver.ServiceNotAuthorizedException;
import org.eclipse.jgit.transport.resolver.ServiceNotEnabledException;

import com.cloudata.auth.AuthenticatedUser;
import com.cloudata.git.model.GitRepository;
import com.cloudata.git.model.GitUser;
import com.cloudata.git.services.GitRepositoryStore;
import com.google.common.base.Preconditions;

class GitRepositoryResolver implements RepositoryResolver<HttpServletRequest> {

  private final boolean autoCreate = true;
  private final GitRepositoryStore gitRepositoryStore;

  public GitRepositoryResolver(GitRepositoryStore gitRepositoryStore) {
    Preconditions.checkNotNull(gitRepositoryStore);
    this.gitRepositoryStore = gitRepositoryStore;
  }

  @Override
  public Repository open(HttpServletRequest req, String projectName) throws RepositoryNotFoundException,
      ServiceNotAuthorizedException, ServiceNotEnabledException {
    while (projectName.endsWith("/")) {
      projectName = projectName.substring(0, projectName.length() - 1);
    }

    if (projectName.endsWith(".git")) {
      // Be nice and drop the trailing ".git" suffix, which we never
      // keep
      // in our database, but clients might mistakenly provide anyway.
      //
      projectName = projectName.substring(0, projectName.length() - 4);
      while (projectName.endsWith("/")) {
        projectName = projectName.substring(0, projectName.length() - 1);
      }
    }

    boolean mustExist = false;

    AuthenticatedUser authenticatedUser = (AuthenticatedUser) req.getAttribute(AuthenticatedUser.class.getName());
    final GitUser user;
    if (authenticatedUser != null) {
      user = gitRepositoryStore.toGitUser(authenticatedUser);
    } else {
      user = null;
    }

    GitRepository repo;
    try {
      repo = gitRepositoryStore.findRepo(projectName);
    } catch (IOException e) {
      throw new RepositoryNotFoundException(projectName, e);
    }

    if (repo == null) {
      if (autoCreate) {
        if (user == null) {
          throw new ServiceNotAuthorizedException();
        }
        try {
          repo = gitRepositoryStore.createRepo(user, projectName);
        } catch (IOException e) {
          throw new RepositoryNotFoundException(projectName, e);
        }
      } else {
        throw new RepositoryNotFoundException(projectName);
      }
    }

    if (user == null && (repo == null || !repo.isPublicRead())) {
      // We want it to prompt for a password
      throw new ServiceNotAuthorizedException();
    }

    Repository opened;
    try {
      opened = gitRepositoryStore.openRepository(user, repo, mustExist);
    } catch (IOException e) {
      RepositoryNotFoundException e2 = new RepositoryNotFoundException("Cannot open repository " + projectName);
      e2.initCause(e);
      throw e2;
    }

    if (opened == null) {
      throw new RepositoryNotFoundException(projectName);
    }
    return opened;
  }
}