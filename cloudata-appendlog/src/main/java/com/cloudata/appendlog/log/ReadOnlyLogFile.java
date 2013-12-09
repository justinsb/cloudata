package com.cloudata.appendlog.log;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;

public class ReadOnlyLogFile extends LogFile {
    final MappedByteBuffer data;
    final LogFileHeader header;

    public ReadOnlyLogFile(File file, long streamOffset) throws IOException {
        super(file, streamOffset);
        this.data = mmap(file);

        this.header = new LogFileHeader(data);
    }

    private MappedByteBuffer mmap(File file) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            FileChannel fc = raf.getChannel();

            long length = file.length();
            long position = 0;
            MappedByteBuffer mmap = fc.map(MapMode.READ_ONLY, position, length);
            return mmap;
        }
    }

    @Override
    public ByteBuffer find(long offset) {
        long position = offset - streamOffset;

        if (position < 0) {
            return null;
        }

        position += LogFileHeader.HEADER_SIZE;

        if (position >= data.limit()) {
            return null;
        }

        ByteBuffer slice = data.duplicate();
        slice.position((int) position);
        return slice;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public long findSupremum() {
        return streamOffset + file.length() - LogFileHeader.HEADER_SIZE;
    }

    @Override
    public Iterator<LogMessage> iterator() {
        return new LogMessageIterator(data);
    }

    @Override
    public LogMessageIterable readLog(long streamOffset, int maxCount) {
        ByteBuffer slice = find(streamOffset);
        if (slice == null) {
            return LogMessageIterable.EMPTY;
        }
        return new LogMessageIterable(slice);
    }

}
