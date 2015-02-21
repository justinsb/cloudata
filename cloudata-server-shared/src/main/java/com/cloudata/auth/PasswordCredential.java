package com.cloudata.auth;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

public class PasswordCredential {
  final String passwordPlaintext;
  String passwordSha256;

  public PasswordCredential(String passwordPlaintext, String passwordSha256) {
    super();
    this.passwordPlaintext = passwordPlaintext;
    this.passwordSha256 = passwordSha256;
  }

  public static PasswordCredential fromSha256(String passwordSha256) {
    return new PasswordCredential(null, passwordSha256);
  }

  public static PasswordCredential fromPlaintext(String password) {
    return new PasswordCredential(password, null);
  }

  public String getSha256() {
    if (passwordSha256 == null) {
      passwordSha256 = Hashing.sha256().hashBytes(passwordPlaintext.getBytes(Charsets.UTF_8)).toString();
    }
    return passwordSha256;
  }

}
