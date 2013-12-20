package com.cloudata.git.jgit;

import java.io.File;
import java.io.IOException;

import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryBuilder;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;

import com.cloudata.clients.keyvalue.KeyValuePath;
import com.cloudata.objectstore.ObjectStorePath;

public class CloudDfsRepository extends DfsRepository {

    final CloudObjDatabase objdb;
    final CloudRefDatabase refdb;

    public CloudDfsRepository(DfsRepositoryDescription repoDesc, ObjectStorePath repoPath, KeyValuePath refsPath,
            File tempDir) {
        super(new DfsRepositoryBuilder<DfsRepositoryBuilder, CloudDfsRepository>() {
            @Override
            public CloudDfsRepository build() throws IOException {
                throw new UnsupportedOperationException();
            }
        }.setRepositoryDescription(repoDesc));

        this.objdb = new CloudObjDatabase(this, repoPath, tempDir);
        this.refdb = new CloudRefDatabase(this, refsPath);
    }

    @Override
    public DfsObjDatabase getObjectDatabase() {
        return objdb;
    }

    @Override
    public DfsRefDatabase getRefDatabase() {
        return refdb;
    }

}
