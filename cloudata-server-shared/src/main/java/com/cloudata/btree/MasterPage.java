package com.cloudata.btree;

import java.nio.ByteBuffer;

public class MasterPage {
    public final static int SIZE = 16;

    final ByteBuffer buffer;
    final int offset;

    public MasterPage(ByteBuffer buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
    }

    public int getRoot() {
        return buffer.getInt(offset);
    }

    public static void create(ByteBuffer mmap, int rootPage, int transactionPage, long transactionId) {
        mmap.putInt(rootPage);
        mmap.putInt(transactionPage);
        mmap.putLong(transactionId);
    }

    public int getTransactionPageId() {
        return buffer.getInt(offset + 4);
    }

    public long getTransactionId() {
        return buffer.getLong(offset + 8);
    }
}
