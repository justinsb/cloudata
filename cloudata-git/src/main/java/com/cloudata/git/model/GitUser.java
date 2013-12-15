package com.cloudata.git.model;

import java.io.IOException;

import com.cloudata.objectstore.ObjectStorePath;

public abstract class GitUser {

    public abstract String getId();

    public abstract boolean canAccess(GitRepository repo) throws IOException;

    public abstract String mapToAbsolutePath(String name);

    public abstract ObjectStorePath buildObjectStorePath(String absolutePath);

}
