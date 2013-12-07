package com.cloudata.appendlog.log;

import java.nio.ByteBuffer;

import org.apache.hadoop.util.PureJavaCrc32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.util.Crc;

/**
 * Accessor class for a message in a mmap-ed log or ByteBuffer.
 * 
 * Format is:
 * 
 * 0: CRC32-C of remainder of message
 * 
 * 4: Total length of message (including header with CRC)
 * 
 * 8: Magic value (currently 0)
 * 
 * 12: Reserved
 */
public class LogMessage {
    private static final Logger log = LoggerFactory.getLogger(LogMessage.class);

    static final int HEADER_SIZE = 16;

    final ByteBuffer buffer;
    final int length;
    long checksum;

    public static LogMessage read(ByteBuffer buffer) {
        if (buffer.remaining() < HEADER_SIZE) {
            log.debug("Remaining < HEADER_SIZE");
            return null;
        }
        LogMessage message = new LogMessage(buffer.duplicate());
        if (!message.isValid()) {
            log.debug("Message not valid");
            return null;
        }
        buffer.position(buffer.position() + message.length);
        return message;
    }

    public LogMessage(ByteBuffer buffer) {
        this.buffer = buffer;
        this.length = buffer.getInt(buffer.position() + 4);
    }

    public boolean isValid() {
        if (length > buffer.remaining() || length < HEADER_SIZE) {
            log.info("Bad length: {}", length);
            return false;
        }
        if (checksum == 0) {
            PureJavaCrc32C crc = new PureJavaCrc32C();
            Crc.update(crc, buffer, buffer.position() + 4, length - 4);
            checksum = crc.getValue();
        }
        long storedChecksum = buffer.getInt(buffer.position() + 0) & 0xffffffffL;
        if (checksum != storedChecksum) {
            log.info("Bad checksum: {} vs {}", checksum, storedChecksum);
            return false;
        }
        return true;
    }

    public ByteBuffer getValue() {
        ByteBuffer value = buffer.duplicate();
        value.position(value.position() + HEADER_SIZE);
        value.limit(value.position() + length - HEADER_SIZE);
        return value;
    }

    public int size() {
        return length;
    }

    public void copyTo(ByteBuffer dest) {
        dest.put(buffer.duplicate());
    }

    public static LogMessage wrap(ByteBuffer value) {
        int totalLength = HEADER_SIZE + value.remaining();

        ByteBuffer wrapped = ByteBuffer.allocate(totalLength);
        wrapped.putInt(0);
        wrapped.putInt(totalLength);
        wrapped.putInt(0);
        wrapped.putInt(0);

        // TODO: Avoid memcpy by building straight into target buffer
        wrapped.put(value.duplicate());

        PureJavaCrc32C crc = new PureJavaCrc32C();
        Crc.update(crc, wrapped, 4, totalLength - 4);
        long checksum = crc.getValue();

        wrapped.putInt(0, (int) (checksum & 0xffffffffL));

        wrapped.position(0);

        LogMessage message = new LogMessage(wrapped);
        assert message.isValid();

        if (!message.isValid()) {
            log.error("MESSAGE NOT VALID");
        }

        assert message.size() == totalLength;
        return message;
    }

    public long getCrc() {
        long storedChecksum = buffer.getInt(buffer.position() + 0) & 0xffffffffL;
        return storedChecksum;
    }
}
