package com.cloudata.auth;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.cloudata.auth.AuthModel.AuthUser;
import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.DataStoreException;

@Singleton
public class DataStoreAuthenticationManager implements AuthenticationManager {

  final DataStore dataStore;

  @Inject
  public DataStoreAuthenticationManager(DataStore dataStore) {
    this.dataStore = dataStore;
  }

  @Override
  public AuthenticatedUser authenticate(String username, String password) throws Exception {

    // TODO: Scoping?
    final String userId = username;
    // final ObjectStore store = new JCloudsObjectStore(blobStore);

    AuthUser user = dataStore.findOne(AuthUser.newBuilder().setName(username).build());
    if (user == null) {
      return null;
    }

    if (!PasswordHashing.matches(user.getPasswordHashed(), password)) {
      return null;
    }

    return new AuthenticatedUser() {
      @Override
      public String getId() {
        return userId;
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
    AuthUser.Builder b = AuthUser.newBuilder();
    b.setName(username);
    b.setPasswordHashed(PasswordHashing.create(password));
    dataStore.insert(b.build());
  }

  public UserInfo findUser(String username) throws DataStoreException {
    AuthUser user = dataStore.findOne(AuthUser.newBuilder().setName(username).build());
    if (user == null) {
      return null;
    }
    return new UserInfo() {

    };
  }

}
