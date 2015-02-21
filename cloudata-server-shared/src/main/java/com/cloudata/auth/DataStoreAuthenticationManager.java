package com.cloudata.auth;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.Randoms;
import com.cloudata.auth.AuthModel.AuthenticationTokenData;
import com.cloudata.auth.AuthModel.UserCredentialData;
import com.cloudata.auth.AuthModel.UserData;
import com.cloudata.datastore.DataStore;
import com.cloudata.datastore.DataStoreException;
import com.google.protobuf.ByteString;

@Singleton
public class DataStoreAuthenticationManager implements AuthenticationManager {
  static final Logger log = LoggerFactory.getLogger(DataStoreAuthenticationManager.class);

  private static final long TOKEN_EXPIRATION = 1000L * 60L * 60L * 24L * 365L;

  final DataStore dataStore;

  @Inject
  public DataStoreAuthenticationManager(DataStore dataStore) {
    this.dataStore = dataStore;
  }

  @Override
  public AuthenticatedUser authenticate(final String login, PasswordCredential password) throws Exception {

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

    return authenticated(credential.getUserId());

  }

  private AuthenticatedUser authenticated(ByteString userId) throws DataStoreException {

    UserData user = dataStore.findOne(UserData.newBuilder().setId(userId).build());
    if (user == null) {
      log.error("Did not find user {}", userId);
      return null;
    }

    return new AuthenticatedUser() {
      @Override
      public String getName() {
        return user.getName();
      }

      @Override
      public ByteString getUserId() {
        return user.getId();
      }

    };

  }

  public void createUser(String username, PasswordCredential password) throws DataStoreException {
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
    dataStore.addMap(DataStore.Mapping.create(AuthenticationTokenData.getDefaultInstance()).hashKey("id"));
  }

  @Override
  public AuthenticationToken createToken(AuthenticatedUser user) throws DataStoreException {
    AuthenticationTokenData.Builder token = AuthenticationTokenData.newBuilder();
    token.setId(Randoms.buildId());
    token.setUserId(user.getUserId());

    long expiration = System.currentTimeMillis() + TOKEN_EXPIRATION;
    token.setExpiration(expiration);

    AuthenticationTokenData data = token.build();
    dataStore.insert(data);
    return new AuthenticationToken(data);
  }

  @Override
  public void revokeToken(AuthenticationToken authenticationToken) throws DataStoreException {
    AuthenticationTokenData.Builder token = AuthenticationTokenData.newBuilder();
    token.setId(authenticationToken.getId());

    AuthenticationTokenData data = token.build();
    dataStore.delete(data);
  }

  @Override
  public AuthenticatedUser authenticate(AuthenticationToken token) throws DataStoreException {
    if (token.hasExpired()) {
      return null;
    }
    ByteString userId = token.data.getUserId();
    return authenticated(userId);
  }

  @Override
  public AuthenticationToken findToken(ByteString tokenId) throws DataStoreException {
    AuthenticationTokenData.Builder matcher = AuthenticationTokenData.newBuilder();
    matcher.setId(tokenId);
    AuthenticationTokenData data = dataStore.findOne(matcher.build());
    if (data == null) {
      return null;
    }
    return new AuthenticationToken(data);
  }
}
