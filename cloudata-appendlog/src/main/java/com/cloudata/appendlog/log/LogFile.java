package com.cloudata.appendlog.log;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;

public abstract class LogFile implements Closeable, Iterable<LogMessage> {
    protected final File file;
    public final long streamOffset;

    public LogFile(File file, long streamOffset) {
        this.file = file;
        this.streamOffset = streamOffset;
    }

    public abstract ByteBuffer find(long streamOffset);

    public abstract long findSupremum();

    public abstract LogMessageIterable readLog(long streamOffset, int maxCount);
}