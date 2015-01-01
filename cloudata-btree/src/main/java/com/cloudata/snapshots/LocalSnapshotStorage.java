package com.cloudata.snapshots;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;

public class LocalSnapshotStorage implements SnapshotStorage {
  final File baseDir;

  public LocalSnapshotStorage(File baseDir) throws IOException {
    super();
    this.baseDir = baseDir;
    if (!baseDir.exists() && !baseDir.mkdirs()) {
      throw new IOException("Unable to create the directory: " + baseDir);
    }
  }

  @Override
  public SnapshotUpload doUpload() {
    return new LocalSnapshotUpload();
  }

  public class LocalSnapshotUpload implements SnapshotUpload {

    final File file;
    final String id;

    public LocalSnapshotUpload() {
      this.id = UUID.randomUUID().toString();
      this.file = new File(baseDir, id);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public ByteSink asSink() {
      return Files.asByteSink(this.file);
    }

    @Override
    public String getId() {
      return this.id;
    }

  }

  @Override
  public ByteSource retrieveSnapshot(String id) {
    File file = new File(baseDir, id);
    return Files.asByteSource(file);
  }

}
