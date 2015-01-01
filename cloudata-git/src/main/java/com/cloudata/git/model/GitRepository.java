package com.cloudata.git.model;

import com.cloudata.git.GitModel.RepositoryData;

public class GitRepository {

  final String objectPath;
  final RepositoryData data;

  public GitRepository(String objectPath, RepositoryData data) {
    this.objectPath = objectPath;
    this.data = data;
  }

  public boolean isPublicRead() {
    return false;
  }

  public String getObjectPath() {
    return objectPath;
  }

  public RepositoryData getData() {
    return data;
  }

}
