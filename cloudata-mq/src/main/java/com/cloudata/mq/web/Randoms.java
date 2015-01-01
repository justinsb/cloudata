package com.cloudata.mq.web;

import java.security.SecureRandom;

import com.google.protobuf.ByteString;

public class Randoms {

  static final SecureRandom random = new SecureRandom();

  private static final int ID_LENGTH = 16;

  public static ByteString buildId() {
    byte[] data = new byte[ID_LENGTH];

    synchronized (random) {
      random.nextBytes(data);
    }

    return ByteString.copyFrom(data);
  }

}
