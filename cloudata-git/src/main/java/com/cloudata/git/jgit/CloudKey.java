package com.cloudata.git.jgit;

import java.io.IOException;

import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.RepositoryCache.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.git.model.GitRepository;
import com.cloudata.git.model.GitUser;

/** Location of a Cloud-backed Repository. */
public class CloudKey implements Key {
    private static final Logger log = LoggerFactory.getLogger(CloudKey.class);

    private final GitUser user;
    private final String uniqueId;
    private final CloudGitRepositoryStore manager;

    private final GitRepository repo;

    public CloudKey(GitUser user, GitRepository repo, CloudGitRepositoryStore manager) {
        this.user = user;
        this.repo = repo;
        this.manager = manager;

        // For now, we store each user separately
        log.warn("Storing each user separtely");
        this.uniqueId = user.getId() + ":" + repo.getAbsolutePath();
    }

    @Override
    public Repository open(final boolean mustExist) throws IOException {
        return manager.openUncached(user, repo, mustExist);
    }

    @Override
    public int hashCode() {
        return uniqueId.hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        return o instanceof CloudKey && uniqueId.equals(((CloudKey) o).uniqueId);
    }

    @Override
    public String toString() {
        return uniqueId.toString();
    }
}