package com.cloudata.git.jgit;

import java.io.File;
import java.io.IOException;

import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryBuilder;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;

import com.cloudata.datastore.DataStore;
import com.cloudata.git.GitModel.RepositoryData;
import com.cloudata.objectstore.ObjectStorePath;

public class CloudDfsRepository extends DfsRepository {

  final RepositoryData data;

  final CloudObjDatabase objdb;
  final CloudRefDatabase refdb;

  public CloudDfsRepository(RepositoryData data, DfsRepositoryDescription repoDesc, ObjectStorePath repoPath,
      DataStore dataStore, File tempDir) {
    super(new DfsRepositoryBuilder<DfsRepositoryBuilder, CloudDfsRepository>() {
      @Override
      public CloudDfsRepository build() throws IOException {
        throw new UnsupportedOperationException();
      }
    }.setRepositoryDescription(repoDesc));
    this.data = data;

    this.objdb = new CloudObjDatabase(this, repoPath, tempDir);
    this.refdb = new CloudRefDatabase(this, dataStore);
  }

  @Override
  public DfsObjDatabase getObjectDatabase() {
    return objdb;
  }

  @Override
  public DfsRefDatabase getRefDatabase() {
    return refdb;
  }

  public RepositoryData getData() {
    return this.data;
  }

}
