package com.cloudata.git.model;

import java.io.IOException;

import com.cloudata.auth.AuthenticatedUser;
import com.cloudata.objectstore.ObjectStorePath;

public abstract class GitUser implements AuthenticatedUser {

    public abstract String getId();

    public abstract boolean canAccess(GitRepository repo) throws IOException;

    public abstract String mapToAbsolutePath(String name);

    public abstract ObjectStorePath buildObjectStorePath(String absolutePath);

}
