package com.cloudata.git;

import java.net.InetSocketAddress;

import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.Providers;

import com.cloudata.git.jgit.CloudGitRepositoryStore;
import com.cloudata.git.keyvalue.KeyValuePath;
import com.cloudata.git.keyvalue.KeyValueStore;
import com.cloudata.git.keyvalue.RedisKeyValueStore;
import com.cloudata.git.services.AuthenticationManager;
import com.cloudata.git.services.GitRepositoryStore;
import com.cloudata.git.services.JCloudsAuthenticationManager;
import com.google.inject.AbstractModule;
import com.google.protobuf.ByteString;

public class GitModule extends AbstractModule {

    @Override
    protected void configure() {
        InetSocketAddress redisSocketAddress = new InetSocketAddress("localhost", 6379);
        KeyValueStore store = new RedisKeyValueStore(redisSocketAddress);
        KeyValuePath refsBase = new KeyValuePath(store, ByteString.copyFromUtf8("gitrefs"));
        GitRepositoryStore repositoryStore = new CloudGitRepositoryStore(refsBase);
        bind(GitRepositoryStore.class).toInstance(repositoryStore);

        ProviderMetadata cloudProviderMetadata = Providers.withId("cloudfiles-us");
        AuthenticationManager authenticationManager = new JCloudsAuthenticationManager(cloudProviderMetadata);
        bind(AuthenticationManager.class).toInstance(authenticationManager);
    }

}
