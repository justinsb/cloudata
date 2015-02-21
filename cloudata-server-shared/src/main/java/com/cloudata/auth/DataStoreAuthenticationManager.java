package com.cloudata.auth;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.Randoms;
import com.cloudata.auth.AuthModel.UserCredentialData;
import com.cloudata.auth.AuthModel.UserData;
import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.DataStoreException;
import com.google.protobuf.ByteString;

@Singleton
public class DataStoreAuthenticationManager implements AuthenticationManager {
  static final Logger log = LoggerFactory.getLogger(DataStoreAuthenticationManager.class);

  final DataStore dataStore;

  @Inject
  public DataStoreAuthenticationManager(DataStore dataStore) {
    this.dataStore = dataStore;
  }

  @Override
  public AuthenticatedUser authenticate(final String login, String password) throws Exception {

    // TODO: Scoping?
    // final String userId = username;
    // final ObjectStore store = new JCloudsObjectStore(blobStore);

    UserCredentialData credential = dataStore.findOne(UserCredentialData.newBuilder().setLogin(login).build());
    if (credential == null) {
      return null;
    }

    if (!HashedPassword.matches(credential.getPasswordHashed(), password)) {
      return null;
    }

    UserData user = dataStore.findOne(UserData.newBuilder().setId(credential.getUserId()).build());
    if (user == null) {
      log.error("Did not find user for credential {}", credential);
      return null;
    }

    return new AuthenticatedUser() {
      @Override
      public String getName() {
        return login;
      }

      @Override
      public ByteString getUserId() {
        return user.getId();
      }

      // @Override
      // public boolean canAccess(GitRepository repo) throws IOException {
      // return CloudGitRepositoryStore.canAccess(this, repo);
      // }
      //
      // @Override
      // public String mapToAbsolutePath(String name) {
      // log.warn("TODO: Escaping?");
      //
      // String path = name;
      // // path = Escaping.escape(name);
      //
      // // ObjectStorePath base = user.getRepoBasePath();
      // //
      // // ObjectStorePath repoPath = base.child(Escaping.escape(name));
      //
      // return path;
      // }
      //
      // @Override
      // public ObjectStorePath buildObjectStorePath(String absolutePath) {
      // return new ObjectStorePath(store, absolutePath);
      // }

    };

  }

  public void createUser(String username, String password) throws DataStoreException {
    UserData.Builder user = UserData.newBuilder();
    user.setId(Randoms.buildId());
    user.setName(username);

    UserCredentialData.Builder credential = UserCredentialData.newBuilder();
    credential.setLogin(username);
    credential.setPasswordHashed(HashedPassword.build(password));
    credential.setUserId(user.getId());

    dataStore.insert(user.build());
    dataStore.insert(credential.build());
  }

  public UserCredential findUserByLogin(String login) throws DataStoreException {
    UserCredentialData data = dataStore.findOne(UserCredentialData.newBuilder().setLogin(login).build());
    if (data == null) {
      return null;
    }
    return new UserCredential(data);
  }

  public static void addMappings(DataStore dataStore) throws DataStoreException {
    dataStore.addMap(DataStore.Mapping.create(UserCredentialData.getDefaultInstance()).hashKey("login"));
    dataStore.addMap(DataStore.Mapping.create(UserData.getDefaultInstance()).hashKey("id"));
  }
}
