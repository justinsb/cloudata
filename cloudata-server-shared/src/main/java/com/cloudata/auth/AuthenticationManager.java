package com.cloudata.auth;

import com.cloudata.datastore.DataStoreException;
import com.google.protobuf.ByteString;

public interface AuthenticationManager {

  AuthenticatedUser authenticate(String username, PasswordCredential passwordCredential) throws Exception;

  AuthenticatedUser authenticate(AuthenticationToken token) throws DataStoreException;

  AuthenticationToken createToken(AuthenticatedUser user) throws DataStoreException;

  void revokeToken(AuthenticationToken authenticationToken) throws DataStoreException;

  AuthenticationToken findToken(ByteString tokenId) throws DataStoreException;

}
