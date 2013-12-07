package com.cloudata.appendlog.log;

import java.nio.ByteBuffer;

/**
 * Accessor class for a LogFile header.
 * 
 * Format is:
 * 
 * 0: Format code. 0 if not yet finalized, then 1 (currently)
 * 
 * 4: CRC-32C of file (Merkle-tree style?)
 * 
 * 8: Length of file (including header)
 * 
 * 16: Highest source-log record id in file
 */
public class LogFileHeader {
    static final int HEADER_SIZE = 256;

    static final int FORMAT_V1 = 1;

    final ByteBuffer buffer;
    final int length;

    public LogFileHeader(ByteBuffer buffer) {
        this.buffer = buffer;
        this.length = buffer.getInt(buffer.position() + 8);
    }

    public boolean isValid() {
        if (length > buffer.remaining() || length < HEADER_SIZE) {
            return false;
        }
        // if (checksum == 0) {
        // PureJavaCrc32C crc = new PureJavaCrc32C();
        // crc.update(buffer.array(), buffer.arrayOffset() + 4, length - 4);
        // checksum = crc.getValue();
        // }
        // long storedChecksum = buffer.getInt(0) & 0xffffffffL;
        // return checksum == storedChecksum;
        return true;
    }

    // public ByteBuffer getValue() {
    // ByteBuffer value = buffer.duplicate();
    // value.position(value.position() + HEADER_SIZE);
    // value.limit(value.position() + length);
    // return value;
    // }

    public void copyTo(ByteBuffer dest) {
        dest.put(buffer.duplicate());
    }

    public static LogFileHeader create(int checksum, long length, long sourceLogRecordId) {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.putInt(FORMAT_V1);
        buffer.putInt(checksum);
        buffer.putLong(length);
        buffer.putLong(sourceLogRecordId);

        buffer.position(0);
        buffer.limit(HEADER_SIZE);

        return new LogFileHeader(buffer);
    }
}
