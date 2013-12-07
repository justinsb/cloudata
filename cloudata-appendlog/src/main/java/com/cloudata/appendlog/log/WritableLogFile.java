package com.cloudata.appendlog.log;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.util.Mmap;

public class WritableLogFile extends LogFile {

    private static final Logger log = LoggerFactory.getLogger(WritableLogFile.class);

    MappedByteBuffer mmap;
    int syncPosition = -1;
    int writePosition;
    int checksum;

    public WritableLogFile(File file, long streamOffset, long length) throws IOException {
        super(file, streamOffset);

        log.info("Creating writable log file: {}", file);

        this.mmap = Mmap.mmapFile(file, length);
        this.writePosition = LogFileHeader.HEADER_SIZE;
        this.mmap.position(this.writePosition);
    }

    public synchronized void commit() throws IOException {
        sync();

        long length = writePosition;

        long sourceLogRecordId = 0;
        LogFileHeader header = LogFileHeader.create(checksum, length, sourceLogRecordId);
        ByteBuffer dest = mmap.duplicate();
        dest.position(0);
        header.copyTo(dest);

        mmap.force();

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(length);
        }

        this.mmap = (MappedByteBuffer) mmap.asReadOnlyBuffer();

        // try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
        // FileChannel fc = raf.getChannel();
        // this.mmap = fc.map(MapMode.READ_ONLY, 0, length);
        // }
    }

    @Override
    public ByteBuffer find(long offset) {
        long position = offset - streamOffset;

        if (position < LogFileHeader.HEADER_SIZE) {
            return null;
        }

        MappedByteBuffer mmap;
        int writePosition;
        synchronized (this) {
            mmap = this.mmap;
            writePosition = this.writePosition;
        }

        if (position >= writePosition) {
            return null;
        }

        ByteBuffer slice = mmap.duplicate();
        slice.position((int) position);
        return slice;
    }

    void sync() {
        synchronized (this) {
            if (syncPosition >= writePosition) {
                return;
            }

            // TODO: I wonder if we can keep this map around, open up the second map and start using it immediately,
            // and add this mmap flush to a 'pending operation' list which we run in the background.
            mmap.force();
            syncPosition = Math.max(syncPosition, writePosition);
        }
    }

    void append(/* long sourceLogRecordId, */LogMessage data) throws IOException {
        synchronized (this) {
            // if (this.sourceLogRecordId >= sourceLogRecordId) {
            // throw new IllegalArgumentException();
            // }
            // this.sourceLogRecordId = sourceLogRecordId;

            assert writePosition == mmap.position();

            if (data.size() > mmap.remaining()) {
                sync();

                long newFileSize = file.length();
                long increment = newFileSize / 4;
                increment = Math.max(data.size() + (1024L * 1024L), increment);

                newFileSize += increment;

                log.info("Growing file {} to {}", file, newFileSize);

                mmap = Mmap.mmapFile(file, newFileSize);

                assert mmap.remaining() >= data.size();
            }

            data.copyTo(mmap);
            writePosition = mmap.position();
        }
    }

    @Override
    public void close() throws IOException {
        sync();
    }

    @Override
    public Iterator<LogMessage> iterator() {
        MappedByteBuffer mmap;
        int writePosition;
        synchronized (this) {
            mmap = this.mmap;
            writePosition = this.writePosition;
        }

        ByteBuffer slice = mmap.duplicate();
        slice.limit(writePosition);
        slice.position(LogFileHeader.HEADER_SIZE);
        return new LogMessageIterator(slice);
    }

    @Override
    public long findSupremum() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogMessageIterable readLog(long streamOffset, int maxCount) {
        ByteBuffer slice = find(streamOffset);
        if (slice == null) {
            log.debug("No entry found @{}", streamOffset);
            return LogMessageIterable.EMPTY;
        }
        log.info("Building iterable: {} {}", slice.position(), slice.limit());
        return new LogMessageIterable(slice);
    }

}
