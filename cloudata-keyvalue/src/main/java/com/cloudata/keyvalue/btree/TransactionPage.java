package com.cloudata.keyvalue.btree;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.List;

import com.cloudata.keyvalue.KeyValueProto.KvAction;

public class TransactionPage extends Page {
    public static final byte PAGE_TYPE = 'T';

    public static final int SIZE = 20;

    long transactionId;
    int freelistPage;
    int rootPage;
    int previousTransactionPage;

    boolean dirty;

    TransactionPage(Page parent, int pageNumber, ByteBuffer buffer) {
        super(parent, pageNumber, buffer);

        if (parent != null) {
            throw new IllegalArgumentException();
        }

        this.transactionId = buffer.getLong(0);
        this.rootPage = buffer.getInt(8);
        this.freelistPage = buffer.getInt(12);
        this.previousTransactionPage = buffer.getInt(16);
        this.dirty = false;
    }

    @Override
    public void write(ByteBuffer dest) {
        dest.putLong(this.transactionId);
        dest.putInt(this.rootPage);
        dest.putInt(this.freelistPage);
        dest.putInt(this.previousTransactionPage);
    }

    public static TransactionPage createNew(int pageNumber, long transactionId) {
        ByteBuffer data = ByteBuffer.allocate(SIZE);
        data.putLong(transactionId);
        data.putInt(0);
        data.putInt(0);
        data.putInt(0);

        data.flip();

        return new TransactionPage(null, pageNumber, data);
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
        return dirty;
    }

    @Override
    public int getSerializedSize() {
        return SIZE;
    }

    @Override
    public byte getPageType() {
        return PAGE_TYPE;
    }

    @Override
    public void dump(PrintStream os) {
        throw new UnsupportedOperationException();
    }

    public void setRootPageId(int newRootPage) {
        this.rootPage = newRootPage;
        this.dirty = true;
    }

    public int getRootPage() {
        return rootPage;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setPreviousTransactionPage(int previousTransactionPage) {
        this.previousTransactionPage = previousTransactionPage;
        this.dirty = true;
    }

    @Override
    public List<Page> split(WriteTransaction txn) {
        throw new IllegalStateException();
    }

    @Override
    public boolean shouldSplit() {
        return false;
    }

}
