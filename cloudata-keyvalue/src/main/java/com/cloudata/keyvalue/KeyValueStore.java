package com.cloudata.keyvalue;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import org.robotninjas.barge.proto.RaftEntry.AppDataKey;
import org.robotninjas.barge.proto.RaftEntry.SnapshotFileInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.Btree;
import com.cloudata.btree.BtreeQuery;
import com.cloudata.btree.Database;
import com.cloudata.btree.Keyspace;
import com.cloudata.btree.ReadOnlyTransaction;
import com.cloudata.btree.WriteTransaction;
import com.cloudata.keyvalue.operation.KeyValueOperation;
import com.cloudata.snapshots.SnapshotStorage;
import com.cloudata.values.Value;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.ByteString;

public class KeyValueStore implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(KeyValueStore.class);

  final Btree btree;
  final Database db;

  public KeyValueStore(File dir, boolean uniqueKeys, SnapshotStorage snapshotStorage, SnapshotFileInfo snapshotFileInfo) throws IOException {
    File data = new File(dir, "data");
    this.db = Database.build(data, null, snapshotStorage, snapshotFileInfo);

    log.warn("Building new btree @{}", dir);

    this.btree = new Btree(db, uniqueKeys);
  }

  public void doAction(KeyValueOperation operation) throws IOException {
    if (operation.isReadOnly()) {
      try (ReadOnlyTransaction txn = btree.beginReadOnly()) {
        txn.doAction(btree, operation);
      }
    } else {
      try (WriteTransaction txn = btree.beginReadWrite()) {
        txn.doAction(btree, operation);
        txn.commit();
      }
    }
  }

  public Value get(final ByteBuffer key) {
    try (ReadOnlyTransaction txn = btree.beginReadOnly()) {
      return txn.get(btree, key);
    }
  }

  class Snapshotter implements Callable<SnapshotFileInfo.Builder>, Closeable {
    final ReadOnlyTransaction txn;
    final SnapshotStorage snapshotDest;

    public Snapshotter(ReadOnlyTransaction txn, SnapshotStorage snapshotDest) {
      this.txn = txn;
      this.snapshotDest = snapshotDest;
    }

    @Override
    public SnapshotFileInfo.Builder call() throws Exception {
      SnapshotFileInfo.Builder info = SnapshotFileInfo.newBuilder();

      AppDataKey.Builder transactionId = info.addAppDataKeyBuilder();
      transactionId.setKey("transactionId");
      transactionId.setValue(txn.getSnapshotTransactionId() + "");
      
      String snapshotLocation = btree.writeSnapshot(txn, snapshotDest);
      info.setLocation(snapshotLocation);
      
      return info;
    }

    @Override
    public void close() throws IOException {
      txn.close();
    }

  };

  public ListenableFuture<SnapshotFileInfo.Builder> beginSnapshot(ListeningExecutorService executor, SnapshotStorage dest) {
    ReadOnlyTransaction txn = btree.beginReadOnly();
    try {
      Snapshotter snapshotter = new Snapshotter(txn, dest);
      ListenableFuture<SnapshotFileInfo.Builder> future = executor.submit(snapshotter);
      txn = null;
      return future;
    } finally {
      if (txn != null) {
        txn.close();
      }
    }
  }
  

  public BtreeQuery buildQuery(Keyspace keyspace, boolean stripKeyspace, ByteString keyPrefix) {
    return new BtreeQuery(btree, keyspace, stripKeyspace, keyPrefix);
  }

  @Override
  public void close() throws IOException {
    db.close();
  }



}
