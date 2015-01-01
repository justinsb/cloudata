package com.cloudata.btree;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.snapshots.SnapshotStorage;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;

public class Btree {
    private static final Logger log = LoggerFactory.getLogger(Btree.class);

    final Database db;
    final boolean uniqueKeys;

    final Lock writeLock = new ReentrantLock();

    public Btree(Database db, boolean uniqueKeys) {
        this.db = db;
        this.uniqueKeys = uniqueKeys;
    }

    public WriteTransaction beginReadWrite() {
        writeLock.lock();
        WriteTransaction txn = db.transactionTracker.beginReadWriteTransaction(writeLock);
        return txn;
    }

    public ReadOnlyTransaction beginReadOnly() {
        ReadOnlyTransaction txn = db.transactionTracker.beginReadOnlyTransaction();
        return txn;
    }

    public boolean isUniqueKeys() {
        return uniqueKeys;
    }

    public Database getDb() {
        return db;
    }

    public String writeSnapshot(ReadOnlyTransaction txn, SnapshotStorage snapshotDest) throws IOException {
      PageStore pageStore = db.getPageStore();
      
      try (SnapshotStorage.SnapshotUpload upload = snapshotDest.doUpload()) {
        ByteSource byteSource = pageStore.asByteSource(txn);
        
        ByteSink sink = upload.asSink();
        byteSource.copyTo(sink);
        return upload.getId();
      }
    }


}
