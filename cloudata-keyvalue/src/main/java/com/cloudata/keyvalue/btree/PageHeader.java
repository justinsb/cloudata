package com.cloudata.keyvalue.btree;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageHeader {
    private static final Logger log = LoggerFactory.getLogger(PageHeader.class);

    final static int HEADER_SIZE = 16;

    private static final int OFFSET_TYPE = 0;
    private static final int OFFSET_LENGTH = 4;
    private static final int OFFSET_CRC = 8;

    final ByteBuffer buffer;
    final int offset;

    public PageHeader(ByteBuffer buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
    }

    public byte getPageType() {
        byte pageType = buffer.get(offset + OFFSET_TYPE);
        return pageType;
    }

    public ByteBuffer getPageSlice() {
        int length = buffer.getInt(offset + OFFSET_LENGTH);

        ByteBuffer slice = buffer.duplicate();
        slice.position(offset + HEADER_SIZE);
        slice.limit(offset + HEADER_SIZE + length);

        log.info("Buffer: {} - {}", offset + HEADER_SIZE, offset + HEADER_SIZE + length);
        return slice.slice().asReadOnlyBuffer();
    }

    public static void write(ByteBuffer dest, byte pageType, int length) {
        dest.put(pageType);
        dest.put((byte) 0);
        dest.put((byte) 0);
        dest.put((byte) 0);
        dest.putInt(length);
        int crc = 0;
        dest.putInt(crc);
        dest.putInt(0);
    }

    public int getDataSize() {
        int length = buffer.getInt(offset + OFFSET_LENGTH);
        return length;
    }

}
