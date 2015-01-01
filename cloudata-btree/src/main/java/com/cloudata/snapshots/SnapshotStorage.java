package com.cloudata.snapshots;

import java.io.Closeable;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;

public interface SnapshotStorage {

  public interface SnapshotUpload extends Closeable {

    ByteSink asSink();

    String getId();
    
  }
  
  SnapshotUpload doUpload();

  ByteSource retrieveSnapshot(String id);
}
