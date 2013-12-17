package com.cloudata.btree;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.cloudata.btree.operation.RowOperation;
import com.cloudata.freemap.SpaceMapEntry;
import com.google.common.collect.Lists;

public class TransactionPage extends Page {
    public static final byte PAGE_TYPE = 'T';

    private static final int OFFSET_FORMAT_VERSION = 0;
    private static final int OFFSET_TRANSACTION_ID = 16;
    private static final int OFFSET_ROOT_PAGE_ID = 24;
    private static final int OFFSET_PREVIOUS_TRANSACTION_PAGE_ID = 28;
    private static final int OFFSET_FSM_SNAPSHOT_PAGEID = 32;
    private static final int OFFSET_FREED_COUNT = 36;
    private static final int OFFSET_ALLOCATED_COUNT = 40;
    private static final int HEADER_SIZE = 44;

    private static final short VERSION_1 = 1;

    static class Mutable {
        long transactionId;
        List<SpaceMapEntry> freed = Lists.newArrayList();
        List<SpaceMapEntry> allocated = Lists.newArrayList();
        int rootPageId;
        int previousTransactionPageId;
        int freeSpaceSnapshotId;

        public Mutable(TransactionPage transactionPage) {
            this.transactionId = transactionPage.getTransactionId();
            this.rootPageId = transactionPage.getRootPageId();
            this.previousTransactionPageId = transactionPage.getPreviousTransactionPageId();
            this.freeSpaceSnapshotId = transactionPage.getFreeSpaceSnapshotId();

            int freedCount = transactionPage.getFreedCount();
            this.freed = new ArrayList<SpaceMapEntry>(freedCount);
            for (int i = 0; i < freedCount; i++) {
                this.freed.add(new SpaceMapEntry(transactionPage.getFreedStart(i), transactionPage.getFreedLength(i)));
            }

            int allocatedCount = transactionPage.getAllocatedCount();
            this.allocated = new ArrayList<SpaceMapEntry>(allocatedCount);
            for (int i = 0; i < allocatedCount; i++) {
                this.allocated.add(new SpaceMapEntry(transactionPage.getAllocatedStart(i), transactionPage
                        .getAllocatedLength(i)));
            }
        }

        public void write(ByteBuffer dest) {
            Collections.sort(freed);
            Collections.sort(allocated);

            dest.putShort(VERSION_1);
            // Padding
            dest.putShort((short) 0);
            dest.putInt(0);
            dest.putLong(0);

            dest.putLong(this.transactionId);
            dest.putInt(this.rootPageId);
            dest.putInt(this.previousTransactionPageId);
            dest.putInt(this.freeSpaceSnapshotId);
            dest.putInt(freed.size());
            dest.putInt(allocated.size());

            for (int i = 0; i < freed.size(); i++) {
                dest.putInt(freed.get(i).start);
                dest.putInt(freed.get(i).length);
            }

            for (int i = 0; i < allocated.size(); i++) {
                dest.putInt(allocated.get(i).start);
                dest.putInt(allocated.get(i).length);
            }
        }

        public boolean isDirty() {
            return true;
        }

        public void setPreviousTransactionPageId(int previousTransactionPageId) {
            this.previousTransactionPageId = previousTransactionPageId;
        }

        public int getSerializedSize() {
            return HEADER_SIZE + ((freed.size() + allocated.size()) * 8);
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

        public void addToFreed(Collection<SpaceMapEntry> entries) {
            freed.addAll(entries);
        }

        public int getFreedCount() {
            return freed.size();
        }

        public int getFreedStart(int i) {
            return freed.get(i).start;
        }

        public int getFreedLength(int i) {
            return freed.get(i).length;
        }

        public void addToAllocated(Collection<SpaceMapEntry> entries) {
            allocated.addAll(entries);
        }

        public int getAllocatedCount() {
            return allocated.size();
        }

        public int getAllocatedStart(int i) {
            return allocated.get(i).start;
        }

        public int getAllocatedLength(int i) {
            return allocated.get(i).length;
        }

        public int getFreeSpaceSnapshotId() {
            return freeSpaceSnapshotId;
        }

        public void setFreeSpaceSnapshotId(int freeSpaceSnapshotId) {
            this.freeSpaceSnapshotId = freeSpaceSnapshotId;
        }

    }

    Mutable mutable;

    TransactionPage(Page parent, int pageNumber, ByteBuffer buffer) {
        super(parent, pageNumber, buffer);

        if (parent != null) {
            throw new IllegalArgumentException();
        }
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
        empty.putShort(OFFSET_FORMAT_VERSION, VERSION_1);
        empty.putLong(OFFSET_TRANSACTION_ID, transactionId);

        return new TransactionPage(null, pageNumber, empty);
    }

    @Override
    public boolean walk(Transaction txn, ByteBuffer from, EntryListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> void doAction(Transaction txn, ByteBuffer key, RowOperation<V> operation) {
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
        os.println("TransactionPage: " + getTransactionId());
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

    public void addToFreed(Collection<SpaceMapEntry> entries) {
        getMutable().addToFreed(entries);
    }

    public void addToAllocated(Collection<SpaceMapEntry> entries) {
        getMutable().addToAllocated(entries);
    }

    public int getFreedStart(int i) {
        if (mutable != null) {
            return mutable.getFreedStart(i);
        }
        return buffer.getInt(HEADER_SIZE + (i * 8));
    }

    public int getFreedLength(int i) {
        if (mutable != null) {
            return mutable.getFreedLength(i);
        }
        return buffer.getInt(HEADER_SIZE + (i * 8) + 4);
    }

    public int getAllocatedStart(int i) {
        if (mutable != null) {
            return mutable.getAllocatedStart(i);
        }
        int freedCount = buffer.getInt(OFFSET_FREED_COUNT);

        return buffer.getInt(HEADER_SIZE + ((freedCount + i) * 8));
    }

    public int getAllocatedLength(int i) {
        if (mutable != null) {
            return mutable.getAllocatedLength(i);
        }
        int freedCount = buffer.getInt(OFFSET_FREED_COUNT);

        return buffer.getInt(HEADER_SIZE + ((freedCount + i) * 8) + 4);
    }

    public int getFreedCount() {
        if (mutable != null) {
            return mutable.getFreedCount();
        }
        return buffer.getInt(OFFSET_FREED_COUNT);
    }

    public int getAllocatedCount() {
        if (mutable != null) {
            return mutable.getAllocatedCount();
        }
        return buffer.getInt(OFFSET_ALLOCATED_COUNT);
    }

    public int getFreeSpaceSnapshotId() {
        if (mutable != null) {
            return mutable.getFreeSpaceSnapshotId();
        }
        return buffer.getInt(OFFSET_FSM_SNAPSHOT_PAGEID);
    }

    public void setFreeSpaceSnapshotId(int fsmPageId) {
        getMutable().setFreeSpaceSnapshotId(fsmPageId);

    }

}
