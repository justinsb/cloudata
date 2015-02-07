package com.cloudata.auth;

import com.google.protobuf.ByteString;

public class Project {
  final ByteString id;

  public Project(ByteString id) {
    this.id = id;
  }

  public ByteString getId() {
    return id;
  }

}
