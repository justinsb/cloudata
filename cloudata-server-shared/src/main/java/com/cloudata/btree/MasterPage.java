package com.cloudata.btree;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.hash.Hashing;

public class MasterPage {
    public final static int SIZE = 20;

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

        {
            assert mmap.position() == 16;
            byte[] b = new byte[16];
            ByteBuffer buff = mmap.duplicate();
            buff.position(0);
            buff.get(b);

            byte[] crc = Hashing.crc32().hashBytes(b).asBytes();
            assert crc.length == 4;

            mmap.put(crc);
        }
    }

    public int getTransactionPageId() {
        return buffer.getInt(offset + 4);
    }

    public long getTransactionId() {
        return buffer.getLong(offset + 8);
    }

    public boolean isValid() {
        byte[] b = new byte[16];
        ByteBuffer buff = buffer.duplicate();
        buff.position(offset);
        buff.get(b);

        byte[] crc = Hashing.crc32().hashBytes(b).asBytes();
        assert crc.length == 4;

        byte[] stored = new byte[4];
        buff.get(stored);
        return Arrays.equals(stored, crc);
    }

    @Override
    public String toString() {
        return "MasterPage [getTransactionId()=" + getTransactionId() + ", getTransactionPageId()="
                + getTransactionPageId() + ", getRoot()=" + getRoot() + "]";
    }

}
