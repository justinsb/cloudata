package com.cloudata.keyvalue.btree;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.google.common.collect.Lists;

public class TransactionPage extends Page {
    public static final byte PAGE_TYPE = 'T';

    private static final int OFFSET_FORMAT_VERSION = 0;
    private static final int OFFSET_TRANSACTION_ID = 16;
    private static final int OFFSET_ROOT_PAGE_ID = 24;
    private static final int OFFSET_PREVIOUS_TRANSACTION_PAGE_ID = 28;
    private static final int OFFSET_FREELIST_SIZE = 32;
    private static final int HEADER_SIZE = 36;

    private static final short VERSION_1 = 1;

    static class Mutable {
        long transactionId;
        List<Integer> freelist = Lists.newArrayList();
        int rootPageId;
        int previousTransactionPageId;

        public Mutable(TransactionPage transactionPage) {
            this.transactionId = transactionPage.getTransactionId();
            this.rootPageId = transactionPage.getRootPageId();
            this.previousTransactionPageId = transactionPage.getPreviousTransactionPageId();
            int freelistSize = transactionPage.getFreelistSize();
            this.freelist = new ArrayList<Integer>(freelistSize);
        }

        public void write(ByteBuffer dest) {
            dest.putShort(VERSION_1);
            // Padding
            dest.putShort((short) 0);
            dest.putInt(0);
            dest.putLong(0);

            dest.putLong(this.transactionId);
            dest.putInt(this.rootPageId);
            dest.putInt(this.previousTransactionPageId);
            dest.putInt(freelist.size());

            Collections.sort(freelist);

            for (int i = 0; i < freelist.size(); i++) {
                dest.putInt(freelist.get(i));
            }
        }

        public boolean isDirty() {
            return true;
        }

        public void setPreviousTransactionPageId(int previousTransactionPageId) {
            this.previousTransactionPageId = previousTransactionPageId;
        }

        public int getSerializedSize() {
            return HEADER_SIZE + (freelist.size() * 4);
        }

        public void setRootPageId(int rootPageId) {
            this.rootPageId = rootPageId;
        }

        public long getTransactionId() {
            return transactionId;
        }

        public int getRootPageId() {
            return rootPageId;
        }

        public void addToFreelist(List<Integer> c) {
            freelist.addAll(c);
        }

    }

    Mutable mutable;

    TransactionPage(Page parent, int pageNumber, ByteBuffer buffer) {
        super(parent, pageNumber, buffer);

        if (parent != null) {
            throw new IllegalArgumentException();
        }
    }

    int getFreelistSize() {
        assert mutable == null;
        return buffer.getInt(OFFSET_FREELIST_SIZE);
    }

    int getPreviousTransactionPageId() {
        assert mutable == null;
        return buffer.getInt(OFFSET_PREVIOUS_TRANSACTION_PAGE_ID);
    }

    @Override
    public void write(ByteBuffer dest) {
        assert mutable != null;
        mutable.write(dest);
    }

    public static TransactionPage createNew(int pageNumber, long transactionId) {
        ByteBuffer empty = ByteBuffer.allocate(HEADER_SIZE);
        empty.putShort(VERSION_1);

        // Padding
        empty.putShort((short) 0);
        empty.putInt(0);
        empty.putLong(0);

        empty.putLong(transactionId);
        empty.putInt(0);
        empty.putInt(0);
        empty.putInt(0);
        empty.flip();

        return new TransactionPage(null, pageNumber, empty);
    }

    @Override
    public boolean walk(Transaction txn, ByteBuffer from, EntryListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void doAction(Transaction txn, KvAction action, ByteBuffer key, ByteBuffer value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getKeyLbound() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDirty() {
        if (mutable == null) {
            return false;
        }

        return mutable.isDirty();
    }

    @Override
    public int getSerializedSize() {
        assert mutable != null;
        return mutable.getSerializedSize();
    }

    @Override
    public byte getPageType() {
        return PAGE_TYPE;
    }

    @Override
    public void dump(PrintStream os) {
        throw new UnsupportedOperationException();
    }

    public void setRootPageId(int newRootPageId) {
        getMutable().setRootPageId(newRootPageId);
    }

    private Mutable getMutable() {
        if (mutable == null) {
            mutable = new Mutable(this);
        }
        return mutable;
    }

    public int getRootPageId() {
        if (mutable != null) {
            return mutable.getRootPageId();
        }
        return buffer.getInt(OFFSET_ROOT_PAGE_ID);
    }

    public long getTransactionId() {
        if (mutable != null) {
            return mutable.getTransactionId();
        }
        return buffer.getLong(OFFSET_TRANSACTION_ID);
    }

    public void setPreviousTransactionPageId(int previousTransactionPageId) {
        getMutable().setPreviousTransactionPageId(previousTransactionPageId);
    }

    @Override
    public List<Page> split(WriteTransaction txn) {
        throw new IllegalStateException();
    }

    @Override
    public boolean shouldSplit() {
        return false;
    }

    public void addToFreelist(List<Integer> freelist) {
        getMutable().addToFreelist(freelist);
    }

}
