package com.cloudata.git.model;

import com.cloudata.git.GitModel.RepositoryData;

public class GitRepository {

    final String absolutePath;
    final RepositoryData data;

    public GitRepository(String absolutePath, RepositoryData data) {
        this.absolutePath = absolutePath;
        this.data = data;
    }

    public boolean isPublicRead() {
        return false;
    }

    public String getAbsolutePath() {
        return absolutePath;
    }

}
