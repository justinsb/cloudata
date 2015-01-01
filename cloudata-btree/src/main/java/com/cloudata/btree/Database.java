package com.cloudata.btree;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.robotninjas.barge.proto.RaftEntry.AppDataKey;
import org.robotninjas.barge.proto.RaftEntry.SnapshotFileInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.io.BackingFile;
import com.cloudata.btree.io.CipherSpec;
import com.cloudata.btree.io.EncryptedBackingFile;
import com.cloudata.btree.io.NioBackingFile;
import com.cloudata.snapshots.SnapshotStorage;
import com.cloudata.util.Directory;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;

public class Database implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Database.class);

    final PageStore pageStore;
    final TransactionTracker transactionTracker;

    private Database(PageStore pageStore) throws IOException {
        this.pageStore = pageStore;

        MasterPage latest = pageStore.findLatestMasterPage();
        this.transactionTracker = new TransactionTracker(this, latest);
    }

    public static Database build(File file, byte[] keyBytes, SnapshotStorage snapshotStorage, SnapshotFileInfo snapshotFileInfo) throws IOException {
        CipherSpec cipherSpec = CipherSpec.AES_128;

        boolean usedSnapshot = false;
        
        if (!file.exists()) {
          Directory.mkdirs(file.getParentFile());

          if (snapshotFileInfo != null) {
            ByteSource snapshot = snapshotStorage.retrieveSnapshot(snapshotFileInfo.getLocation());
            
            log.warn("Restoring database @{}", file);
            snapshot.copyTo(Files.asByteSink(file));
            usedSnapshot = true;
          } else {
            long size = 1024L * 1024L * 64L;
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.setLength(size);
            }

            BackingFile backingFile = new NioBackingFile(file);
            if (keyBytes != null) {
                backingFile = new EncryptedBackingFile(backingFile, cipherSpec, keyBytes);
            }

            CachingPageStore.createNew(backingFile);
            backingFile.close();
          }
        }

       
        PageStore pageStore;
        
        {
            BackingFile backingFile = new NioBackingFile(file);
            if (keyBytes != null) {
                backingFile = new EncryptedBackingFile(backingFile, cipherSpec, keyBytes);
            }

            pageStore = CachingPageStore.open(backingFile);
        }
        
        if (snapshotFileInfo != null) {
          long transactionId = getTransactionId(snapshotFileInfo);
          if (pageStore.findLatestMasterPage().getTransactionId() < transactionId) {
            log.warn("Have database, but is out of date");

            if (usedSnapshot) {
              throw new IllegalStateException("Snapshot is corrupt");
            }
            
            pageStore.close();

            // TODO: Copy to temp location for safety?
            file.delete();
            
            // Recurse; we won't do so infinitely because we deleted the file, so we will restore, and if we get here again the usedSnapshot test will throw
            return build(file, keyBytes, snapshotStorage, snapshotFileInfo);
          }
        }
        
        return new Database(pageStore);
    }

    private static long getTransactionId(SnapshotFileInfo snapshotFileInfo) {
      for (AppDataKey appData : snapshotFileInfo.getAppDataKeyList()) {
        if (appData.getKey().equals("transactionId")) {
          return Long.parseLong(appData.getValue());
        }
      }
      throw new IllegalStateException("Not a valid snapshot (no transactionId)");
    }
    
    public PageStore getPageStore() {
        return pageStore;
    }

    @Override
    public void close() throws IOException {
        log.info("Closing down database: {}", pageStore.getPathInfo());
        pageStore.close();
        transactionTracker.close();
    }

}
