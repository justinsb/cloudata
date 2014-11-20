package com.cloudata.btree;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.io.BackingFile;
import com.cloudata.btree.io.CipherSpec;
import com.cloudata.btree.io.EncryptedBackingFile;
import com.cloudata.btree.io.NioBackingFile;
import com.cloudata.util.Directory;
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

    public static Database build(File file, byte[] keyBytes) throws IOException {
        CipherSpec cipherSpec = CipherSpec.AES_128;

        if (!file.exists()) {
          Directory.mkdirs(file.getParentFile());

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

        {
            BackingFile backingFile = new NioBackingFile(file);
            if (keyBytes != null) {
                backingFile = new EncryptedBackingFile(backingFile, cipherSpec, keyBytes);
            }

            PageStore pageStore = CachingPageStore.open(backingFile);
            return new Database(pageStore);
        }
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
