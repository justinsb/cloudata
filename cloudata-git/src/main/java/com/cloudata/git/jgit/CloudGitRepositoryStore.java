// Copyright (C) 2008 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudata.git.jgit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.SecureRandom;

import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.RepositoryCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.clients.keyvalue.KeyValuePath;
import com.cloudata.git.GitModel.RepositoryData;
import com.cloudata.git.model.GitRepository;
import com.cloudata.git.model.GitUser;
import com.cloudata.git.services.GitRepositoryStore;
import com.cloudata.objectstore.ObjectStorePath;
import com.google.common.io.Files;
import com.google.inject.Singleton;
import com.google.protobuf.ByteString;

/** Manages Git repositories stored in the cloud */
@Singleton
public class CloudGitRepositoryStore implements GitRepositoryStore {
    private static final String METADATA_KEY = "metadata";

    static final ByteString ZERO = ByteString.copyFrom(new byte[] { 0 });

    private static final Logger log = LoggerFactory.getLogger(CloudGitRepositoryStore.class);

    private final KeyValuePath refsBase;

    private final File tempDir;

    static final SecureRandom random = new SecureRandom();

    public CloudGitRepositoryStore(KeyValuePath refsBase) {
        this.refsBase = refsBase;

        log.warn("Using new random tempdir");

        this.tempDir = Files.createTempDir();
    }

    public static boolean canAccess(GitUser user, GitRepository repository) throws IOException {
        String absolutePath = repository.getAbsolutePath();
        ObjectStorePath repoPath = user.buildObjectStorePath(absolutePath);
        return loadRepository(repoPath) != null;
    }

    Repository openUncached(GitUser user, GitRepository repo, boolean mustExist) throws IOException {
        String absolutePath = repo.getAbsolutePath();
        ObjectStorePath repoPath = user.buildObjectStorePath(absolutePath);

        ByteString suffix = ZERO.concat(ByteString.copyFromUtf8(absolutePath)).concat(ZERO);
        KeyValuePath refsPath = refsBase.child(suffix);

        DfsRepositoryDescription description = new DfsRepositoryDescription(absolutePath);
        CloudDfsRepository dfs = new CloudDfsRepository(description, repoPath, refsPath, tempDir);

        try {
            if (!dfs.exists()) {
                if (mustExist) {
                    throw new RepositoryNotFoundException(absolutePath);
                }
                dfs.create(true);
                dfs.updateRef(Constants.HEAD).link("refs/heads/master");
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error creating repository", e);
        }

        dfs.getConfig().setBoolean("http", null, "receivepack", true);

        return dfs;
    }

    static GitRepository loadRepository(ObjectStorePath repoPath) throws IOException {
        ObjectStorePath metadataPath = repoPath.child(METADATA_KEY);

        try {
            ByteString data = metadataPath.read();
            RepositoryData repositoryData = RepositoryData.parseFrom(data);
            return new GitRepository(repoPath.getPath(), repositoryData);
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    @Override
    public GitRepository findRepo(GitUser user, String name) throws IOException {
        if (user == null) {
            // No public repos
            return null;
        }

        String absolutePath = user.mapToAbsolutePath(name);
        ObjectStorePath repoPath = user.buildObjectStorePath(absolutePath);
        return loadRepository(repoPath);
    }

    @Override
    public Repository openRepository(GitUser user, GitRepository repo, boolean mustExist) throws IOException {
        final CloudKey loc = new CloudKey(user, repo, this);
        try {
            return RepositoryCache.open(loc, mustExist);
        } catch (IOException e1) {
            RepositoryNotFoundException e2 = new RepositoryNotFoundException("Cannot open repository "
                    + repo.getAbsolutePath());
            e2.initCause(e1);
            throw e2;
        }
    }

    @Override
    public GitRepository createRepo(GitUser user, String name) throws IOException {
        String absolutePath = user.mapToAbsolutePath(name);
        ObjectStorePath repoPath = user.buildObjectStorePath(absolutePath);
        ObjectStorePath metadataPath = repoPath.child(METADATA_KEY);

        RepositoryData.Builder repositoryDataBuilder = RepositoryData.newBuilder();

        byte[] uniqueId = new byte[128];
        synchronized (random) {
            random.nextBytes(uniqueId);
        }
        repositoryDataBuilder.setUniqueId(ByteString.copyFrom(uniqueId));

        RepositoryData repositoryData = repositoryDataBuilder.build();

        metadataPath.create(repositoryData.toByteString());

        return new GitRepository(absolutePath, repositoryData);
    }
}
