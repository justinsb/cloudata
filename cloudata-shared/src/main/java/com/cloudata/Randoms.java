package com.cloudata;

import java.security.SecureRandom;

import com.google.protobuf.ByteString;

public class Randoms {

  static final SecureRandom random = new SecureRandom();

  private static final int ID_LENGTH = 16;

  public static ByteString buildId() {
    return build(ID_LENGTH);
  }

  public static ByteString build(int length) {
    byte[] data = new byte[length];

    synchronized (random) {
      random.nextBytes(data);
    }

    return ByteString.copyFrom(data);
  }

}
