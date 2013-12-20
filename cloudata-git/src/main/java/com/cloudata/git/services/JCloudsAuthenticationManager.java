package com.cloudata.git.services;

import java.io.IOException;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.providers.ProviderMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.auth.AuthenticationManager;
import com.cloudata.git.jgit.CloudGitRepositoryStore;
import com.cloudata.git.model.GitRepository;
import com.cloudata.git.model.GitUser;
import com.cloudata.objectstore.JCloudsObjectStore;
import com.cloudata.objectstore.ObjectStore;
import com.cloudata.objectstore.ObjectStorePath;

public class JCloudsAuthenticationManager implements AuthenticationManager {

    private static final Logger log = LoggerFactory.getLogger(JCloudsAuthenticationManager.class);

    final ProviderMetadata cloudProviderMetadata;

    public JCloudsAuthenticationManager(ProviderMetadata cloudProviderMetadata) {
        super();
        this.cloudProviderMetadata = cloudProviderMetadata;
    }

    @Override
    public GitUser authenticate(String username, String password) throws Exception {
        // TODO: Caching
        // TODO: Context close
        final BlobStoreContext context = ContextBuilder.newBuilder(cloudProviderMetadata)
                .credentials(username, password).buildView(BlobStoreContext.class);

        BlobStore blobStore = context.getBlobStore();

        final String userId = username;
        final ObjectStore store = new JCloudsObjectStore(blobStore);

        return new GitUser() {
            @Override
            public String getId() {
                return userId;
            }

            @Override
            public boolean canAccess(GitRepository repo) throws IOException {
                return CloudGitRepositoryStore.canAccess(this, repo);
            }

            @Override
            public String mapToAbsolutePath(String name) {
                log.warn("TODO: Escaping?");

                String path = name;
                // path = Escaping.escape(name);

                // ObjectStorePath base = user.getRepoBasePath();
                //
                // ObjectStorePath repoPath = base.child(Escaping.escape(name));

                return path;
            }

            @Override
            public ObjectStorePath buildObjectStorePath(String absolutePath) {
                return new ObjectStorePath(store, absolutePath);
            }

        };
    }
}
