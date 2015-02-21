package com.cloudata.auth;

import java.util.Date;

import com.cloudata.auth.AuthModel.AuthenticationTokenData;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class AuthenticationToken {

  final AuthenticationTokenData data;

  public AuthenticationToken(AuthenticationTokenData data) {
    this.data = data;
  }

  public Date getExpiration() {
    if (!data.hasExpiration()) {
      return null;
    }
    long expiration = data.getExpiration();
    return new Date(expiration);
  }

  public ByteString getId() {
    Preconditions.checkState(data.hasId());
    return data.getId();
  }

  public boolean hasExpired() {
    if (!data.hasExpiration()) {
      throw new IllegalStateException();
    }
    long expiration = data.getExpiration();
    return expiration < System.currentTimeMillis();
  }

}
