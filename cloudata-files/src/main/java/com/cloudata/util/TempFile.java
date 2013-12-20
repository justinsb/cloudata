package com.cloudata.util;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.io.ByteSink;
import com.google.common.io.Files;

public class TempFile implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TempFile.class);

    File file;

    public TempFile(File tempDir) {
        this.file = new File(tempDir, UUID.randomUUID().toString());
    }

    public ByteSink asByteSink() {
        Preconditions.checkState(this.file != null);
        return Files.asByteSink(file);
    }

    public HashCode hash(HashFunction hashFunction) throws IOException {
        Preconditions.checkState(this.file != null);
        return Files.hash(file, hashFunction);
    }

    public void renameTo(File path) throws IOException {
        Preconditions.checkState(this.file != null);
        if (!file.renameTo(path)) {
            throw new IOException("Unable to move temporary file");
        }
        file = null;
    }

    @Override
    public void close() {
        if (file != null) {
            if (!file.delete()) {
                log.warn("Unable to delete temporary file");
            } else {
                file = null;
            }
        }
    }

}
