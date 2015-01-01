package com.cloudata.btree;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageHeader {
    private static final Logger log = LoggerFactory.getLogger(PageHeader.class);

    public final static int HEADER_SIZE = 24;

    private static final int OFFSET_TYPE = 0;
    private static final int OFFSET_LENGTH = 4;
    private static final int OFFSET_TRANSACTION_ID = 8;
    private static final int OFFSET_CRC = 16;

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
    
    public long getTransactionId() {
        return buffer.getLong(offset + OFFSET_TRANSACTION_ID);
    }

    public static byte getPageType(ByteBuf buf) {
        byte pageType = buf.getByte(buf.readerIndex() + OFFSET_TYPE);
        return pageType;
    }

    public ByteBuffer getPageSlice() {
        int length = buffer.getInt(offset + OFFSET_LENGTH);

        ByteBuffer slice = buffer.duplicate();
        slice.position(offset + HEADER_SIZE);
        slice.limit(offset + HEADER_SIZE + length);

        // log.info("Buffer: {} - {}", offset + HEADER_SIZE, offset + HEADER_SIZE + length);
        return slice.slice().asReadOnlyBuffer();
    }

    public static void write(ByteBuffer dest, byte pageType, int length, long transactionId) {
        dest.put(pageType);
        dest.put((byte) 0);
        dest.put((byte) 0);
        dest.put((byte) 0);
        dest.putInt(length);
        dest.putLong(transactionId);
        int crc = 0;
        dest.putInt(crc);
        dest.putInt(0);
    }

    public int getDataSize() {
        int length = buffer.getInt(offset + OFFSET_LENGTH);
        return length;
    }

    public static int getDataSize(ByteBuf buf) {
        int length = buf.getInt(buf.readerIndex() + OFFSET_LENGTH);
        return length;
    }

    @Override
    public String toString() {
        return "PageHeader [pageType()=" + getPageType() + ", dataSize()=" + getDataSize() + "]";
    }

}
