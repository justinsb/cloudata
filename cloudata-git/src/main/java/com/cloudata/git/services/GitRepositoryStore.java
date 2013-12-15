package com.cloudata.git.services;

import java.io.IOException;

import org.eclipse.jgit.lib.Repository;

import com.cloudata.git.model.GitRepository;
import com.cloudata.git.model.GitUser;

public interface GitRepositoryStore {
    GitRepository findRepo(GitUser user, String path) throws IOException;

    Repository openRepository(GitUser user, GitRepository repo, boolean mustExist) throws IOException;

    GitRepository createRepo(GitUser owner, String path) throws IOException;

}
