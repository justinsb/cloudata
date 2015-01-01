package com.cloudata.objectstore.s3;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.cloudata.objectstore.ObjectInfo;

public class S3FileInfo extends ObjectInfo {

  final S3ObjectSummary entry;

  public S3FileInfo(S3ObjectSummary entry) {
    this.entry = entry;
  }

  @Override
  public long getLength() {
    return entry.getSize();
  }

  @Override
  public String getPath() {
    return entry.getKey();
  }

  @Override
  public boolean isSubdir() {
    return false;
  }

}
