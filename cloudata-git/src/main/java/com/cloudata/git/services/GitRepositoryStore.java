package com.cloudata.git.services;

import java.io.IOException;
import java.util.List;

import org.eclipse.jgit.lib.Repository;

import com.cloudata.auth.AuthenticatedUser;
import com.cloudata.git.model.GitRepository;
import com.cloudata.git.model.GitUser;

public interface GitRepositoryStore {

  List<GitRepository> listRepos(GitUser user) throws IOException;

  Repository openRepository(GitUser user, GitRepository repo, boolean mustExist) throws IOException;

  GitRepository createRepo(GitUser owner, String path) throws IOException;

  GitUser toGitUser(AuthenticatedUser authenticatedUser);

  GitRepository findRepo(String repoName) throws IOException;

}
