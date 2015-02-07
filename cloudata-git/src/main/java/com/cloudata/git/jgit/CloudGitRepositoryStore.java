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
import java.io.IOException;
import java.security.SecureRandom;

import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.RepositoryCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.DataStoreException;
import com.cloudata.git.GitModel.RefData;
import com.cloudata.git.GitModel.RepositoryData;
import com.cloudata.git.model.GitRepository;
import com.cloudata.git.model.GitUser;
import com.cloudata.git.services.GitRepositoryStore;
import com.cloudata.objectstore.ObjectStore;
import com.cloudata.objectstore.ObjectStorePath;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Files;
import com.google.inject.Singleton;
import com.google.protobuf.ByteString;

/** Manages Git repositories stored in the cloud */
@Singleton
public class CloudGitRepositoryStore implements GitRepositoryStore {
  private static final String METADATA_KEY = "metadata";

  static final ByteString ZERO = ByteString.copyFrom(new byte[] { 0 });

  private static final Logger log = LoggerFactory.getLogger(CloudGitRepositoryStore.class);

  private final DataStore dataStore;

  private final File tempDir;

  static final SecureRandom random = new SecureRandom();

  final ObjectStore objectStore;

  public CloudGitRepositoryStore(ObjectStore objectStore, DataStore dataStore) {
    this.objectStore = objectStore;
    this.dataStore = dataStore;

    log.warn("Using new random tempdir");

    this.tempDir = Files.createTempDir();
  }

  // public static boolean canAccess(GitUser user, GitRepository repository) throws IOException {
  // // ObjectStorePath repoPath = repository.getRepoPath();
  // // return loadRepository(repoPath) != null;
  // }

  Repository openUncached(GitRepository repo, boolean mustExist) throws IOException {
    String objectPath = repo.getObjectPath();

    // ByteString suffix = ZERO.concat(ByteString.copyFromUtf8(absolutePath)).concat(ZERO);
    // KeyValuePath refsPath = refsBase.child(suffix);

    DfsRepositoryDescription description = new DfsRepositoryDescription(objectPath);
    ObjectStorePath repoPath = new ObjectStorePath(objectStore, objectPath);
    CloudDfsRepository dfs = new CloudDfsRepository(repo.getData(), description, repoPath, dataStore, tempDir);

    try {
      if (!dfs.exists()) {
        if (mustExist) {
          throw new RepositoryNotFoundException(repo.getData().getName());
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

  GitRepository loadRepository(String repoName) throws IOException {
    RepositoryData.Builder matcher = RepositoryData.newBuilder();
    matcher.setName(repoName);

    RepositoryData repositoryData = dataStore.findOne(matcher.build());
    if (repositoryData == null) {
      return null;
    }

    String objectPath = toObjectPath(repositoryData);

    return new GitRepository(objectPath, repositoryData);
  }

  @Override
  public GitRepository findRepo(String repoName) throws IOException {
    // if (user == null) {
    // // No public repos
    // return null;
    // }

    return loadRepository(repoName);
  }

  @Override
  public Repository openRepository(GitRepository repo, boolean mustExist) throws IOException {
    final CloudKey loc = new CloudKey(repo, this);
    try {
      return RepositoryCache.open(loc, mustExist);
    } catch (IOException e1) {
      RepositoryNotFoundException e2 = new RepositoryNotFoundException("Cannot open repository " + repo.getObjectPath());
      e2.initCause(e1);
      throw e2;
    }
  }

  @Override
  public GitRepository createRepo(GitUser user, String name) throws IOException {
    // String absolutePath = user.mapToAbsolutePath(name);
    // ObjectStorePath repoPath = buildObjectStorePath(name);

    RepositoryData.Builder repositoryDataBuilder = RepositoryData.newBuilder();

    byte[] uniqueId = new byte[16];
    synchronized (random) {
      random.nextBytes(uniqueId);
    }
    repositoryDataBuilder.setRepositoryId(ByteString.copyFrom(uniqueId));
    repositoryDataBuilder.setName(name);
    repositoryDataBuilder.setOwner(user.getId());

    RepositoryData repositoryData = repositoryDataBuilder.build();
    dataStore.insert(repositoryData);

    String objectPath = toObjectPath(repositoryData);

    GitRepository gitRepository = new GitRepository(objectPath, repositoryData);

    // Create the repo
    openRepository(gitRepository, false);

    return gitRepository;
  }

  private String toObjectPath(RepositoryData repositoryData) {
    return BaseEncoding.base64Url().encode(repositoryData.getRepositoryId().toByteArray());
  }

  public static void addMappings(DataStore dataStore) throws DataStoreException {
    dataStore.addMap(DataStore.Mapping.create(RepositoryData.getDefaultInstance()).hashKey("name"));
    dataStore.addMap(DataStore.Mapping.create(RefData.getDefaultInstance()).hashKey("repository_id").rangeKey("name")
        .filterable("object_id"));
  }
}
