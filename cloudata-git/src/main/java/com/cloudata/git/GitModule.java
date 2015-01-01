package com.cloudata.git;

import java.net.URI;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import com.cloudata.auth.DataStoreAuthenticationManager;
import com.cloudata.auth.ProjectBasicAuthFilter;
import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.DataStoreException;
import com.cloudata.datastore.sql.SqlDataStore;
import com.cloudata.git.jgit.CloudGitRepositoryStore;
import com.cloudata.git.services.GitRepositoryStore;
import com.cloudata.objectstore.ObjectStore;
import com.cloudata.objectstore.s3.S3ObjectStore;
import com.google.common.base.Strings;
import com.google.inject.AbstractModule;

public class GitModule extends AbstractModule {

  @Override
  protected void configure() {
    ObjectStore objectStore = createObjectStore();

    DataSource poolingDataSource = createDataSource();

    DataStore dataStore = new SqlDataStore(poolingDataSource);
    GitRepositoryStore repositoryStore = new CloudGitRepositoryStore(objectStore, dataStore);
    bind(GitRepositoryStore.class).toInstance(repositoryStore);

    // ProviderMetadata cloudProviderMetadata = Providers.withId("cloudfiles-us");
    // AuthenticationManager authenticationManager = new JCloudsAuthenticationManager(cloudProviderMetadata);
    // bind(AuthenticationManager.class).toInstance(authenticationManager);

    DataStoreAuthenticationManager authenticationManager = new DataStoreAuthenticationManager(dataStore);

    {
      String username = System.getenv("ENSURE_USERNAME");
      String password = System.getenv("ENSURE_PASSWORD");
      if (username != null) {
        try {
          if (authenticationManager.findUser(username) == null) {
            authenticationManager.createUser(username, password);
          }
        } catch (DataStoreException e) {
          throw new IllegalStateException("Error creating user", e);
        }
      }
    }
    ProjectBasicAuthFilter projectBasicAuthFilter = new ProjectBasicAuthFilter(authenticationManager, "Git cloud");
    bind(ProjectBasicAuthFilter.class).toInstance(projectBasicAuthFilter);
  }

  private ObjectStore createObjectStore() {
    String objectStore = System.getenv("OBJECT_STORE");
    if (Strings.isNullOrEmpty(objectStore)) {
      throw new IllegalStateException("OBJECT_STORE is not set");
    }
    URI objectStoreUri = URI.create(objectStore);

    String username = objectStoreUri.getUserInfo().split(":")[0];
    String password = objectStoreUri.getUserInfo().split(":")[1];

    String scheme = objectStoreUri.getScheme();
    if (scheme.equals("s3")) {
      String bucket = objectStoreUri.getPath();
      return new S3ObjectStore(bucket, username, password);
    } else {
      throw new IllegalStateException("Unknown scheme: " + scheme);
    }
  }

  private DataSource createDataSource() {
    // InetSocketAddress redisSocketAddress = new InetSocketAddress("localhost", 6379);
    // KeyValueStore store = new RedisKeyValueStore(redisSocketAddress);
    // KeyValuePath refsBase = new KeyValuePath(store, ByteString.copyFromUtf8("gitrefs"));

    // The DATABASE_URL for the Heroku Postgres add-on follows this naming convention:
    // postgres://<username>:<password>@<host>/<dbname>

    String databaseUrl = System.getenv("DATABASE_URL");
    if (Strings.isNullOrEmpty(databaseUrl)) {
      throw new IllegalStateException("DATABASE_URL is not set");
    }
    URI dbUri = URI.create(databaseUrl);

    String dbUsername = dbUri.getUserInfo().split(":")[0];
    String dbPassword = dbUri.getUserInfo().split(":")[1];

    String dbUrl;

    if (!dbUri.getHost().contains(".")) {
      String key = dbUri.getHost();
      key = key.toUpperCase();
      String host = System.getenv(key + "_SERVICE_HOST");
      if (host == null) {
        throw new IllegalStateException("Unable to find service: " + (key + "_SERVICE_HOST"));
      }
      String port = System.getenv(key + "_SERVICE_PORT");
      if (port == null) {
        throw new IllegalStateException("Unable to find service: " + (key + "_SERVICE_PORT"));
      }
      dbUrl = "jdbc:postgresql://" + host + ":" + port + dbUri.getPath();
    } else {
      int port = dbUri.getPort();
      if (port == -1) {
        port = 5432;
      }
      dbUrl = "jdbc:postgresql://" + dbUri.getHost() + ":" + port + dbUri.getPath();
    }
    Properties properties = new Properties();
    properties.put("user", dbUsername);
    properties.put("password", dbPassword);

    ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(dbUrl, properties);

    PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);

    ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
    poolableConnectionFactory.setPool(connectionPool);

    PoolingDataSource<PoolableConnection> dataSource = new PoolingDataSource<>(connectionPool);

    return dataSource;
  }

}
