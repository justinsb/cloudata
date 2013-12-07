package com.cloudata.appendlog.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class LogFileSet {

    private static final Logger log = LoggerFactory.getLogger(LogFileSet.class);

    final TreeMap<Long, LogFile> logFiles;
    WritableLogFile writer;
    private final File baseDir;

    public LogFileSet(File baseDir) throws IOException {
        this.baseDir = baseDir;
        this.logFiles = findLogFiles(baseDir);
    }

    private static TreeMap<Long, LogFile> findLogFiles(File baseDir) throws IOException {
        TreeMap<Long, LogFile> logFiles = Maps.newTreeMap();

        for (File file : baseDir.listFiles()) {
            String name = file.getName();
            if (!name.endsWith(".log")) {
                continue;
            }

            int dotIndex = name.lastIndexOf('.');
            name = name.substring(0, dotIndex);

            long start = Long.valueOf(name, 16);
            logFiles.put(start, new ReadOnlyLogFile(file, start));
        }

        return logFiles;
    }

    private long findSupremum() {
        Entry<Long, LogFile> lastEntry = logFiles.lastEntry();
        if (lastEntry == null) {
            return 0;
        }

        return lastEntry.getValue().findSupremum();
    }

    public synchronized void append(ByteBuffer value) throws IOException {
        if (writer == null) {
            long tail = findSupremum();
            String name = Long.toHexString(tail);
            writer = new WritableLogFile(new File(baseDir, name), tail, 1L * 1024L * 1024L);
        }

        LogMessage message = LogMessage.wrap(value);
        writer.append(message);
    }

    public LogMessageIterable readLog(long logId, long streamOffset, int maxCount) {
        // We special case the writer, as it's the most common place we read from (the latest)
        if (writer != null) {
            if (streamOffset >= writer.streamOffset) {
                return writer.readLog(streamOffset, maxCount);
            }
        }

        Entry<Long, LogFile> floorEntry = logFiles.floorEntry(streamOffset);
        if (floorEntry == null) {
            log.debug("No log for @{}", streamOffset);

            return LogMessageIterable.EMPTY;
        }
        return floorEntry.getValue().readLog(streamOffset, maxCount);
    }
}
