package com.cloudata.objectstore.s3;

import com.cloudata.objectstore.ObjectInfo;

public class S3DirInfo extends ObjectInfo {

  final String path;

  public S3DirInfo(String path) {
    this.path = path;
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public boolean isSubdir() {
    return true;
  }

}
