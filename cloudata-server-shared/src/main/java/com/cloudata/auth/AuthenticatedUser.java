package com.cloudata.auth;

import com.google.protobuf.ByteString;

public interface AuthenticatedUser {

  String getName();

  ByteString getUserId();

}
