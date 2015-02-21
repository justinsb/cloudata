package com.cloudata.auth;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import com.cloudata.auth.AuthModel.HashedPasswordData;
import com.cloudata.auth.AuthModel.HashedPasswordDataOrBuilder;
import com.google.protobuf.ByteString;

public class HashedPassword {
  private static final int DEFAULT_ITERATIONS = 1000;
  private static final int HASH_BYTE_SIZE = 256;
  private static final int SALT_BYTES = 16;

  private static byte[] computeHashed(HashedPasswordDataOrBuilder hashed, String password) {
    int length = HASH_BYTE_SIZE;
    if (hashed.hasHashed()) {
      length = hashed.getHashed().size() * 8;
    }
    PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), hashed.getSalt().toByteArray(), hashed.getIterations(),
        length);
    SecretKeyFactory skf;
    try {
      skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("Cannot find hash algorithm", e);
    }
    try {
      byte[] hash = skf.generateSecret(spec).getEncoded();
      return hash;
    } catch (InvalidKeySpecException e) {
      throw new IllegalStateException("Error hashing password", e);
    }
  }

  public static boolean matches(HashedPasswordData hashed, String password) {
    byte[] hash = computeHashed(hashed, password);
    return constantTimeIsEqual(hash, hashed.getHashed().toByteArray());
  }

  public static boolean constantTimeIsEqual(byte[] a, byte[] b) {
    if (a.length != b.length) {
      return false;
    }

    int result = 0;
    for (int i = 0; i < a.length; i++) {
      result |= a[i] ^ b[i];
    }
    return result == 0;
  }

  static SecureRandom secureRandom = new SecureRandom();

  public static HashedPasswordData build(String password) {
    HashedPasswordData.Builder b = HashedPasswordData.newBuilder();
    b.setIterations(DEFAULT_ITERATIONS);
    byte[] salt = new byte[SALT_BYTES];
    synchronized (secureRandom) {
      secureRandom.nextBytes(salt);
    }
    b.setSalt(ByteString.copyFrom(salt));
    byte[] hash = computeHashed(b, password);
    b.setHashed(ByteString.copyFrom(hash));
    return b.build();
  }
}
