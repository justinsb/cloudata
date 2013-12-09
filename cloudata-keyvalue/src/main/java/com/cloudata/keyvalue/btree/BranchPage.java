package com.cloudata.keyvalue.btree;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.cloudata.keyvalue.KeyValueProto.KvAction;

public class BranchPage extends Page {
    public static final byte PAGE_TYPE = 'B';

    private static final int INDEX_ENTRY_SIZE = 6;

    Mutable mutable;

    public BranchPage(Page parent, int pageNumber, ByteBuffer buffer) {
        super(parent, pageNumber, buffer);
    }

    class Mutable {
        final List<Entry> entries;
        int totalKeySize;

        private int lbound(ByteBuffer find) {
            int findLength = find.remaining();

            int min = 0;

            int length = getEntryCount();
            while (length > 0) {
                int half = length >> 1;
                int mid = min + half;

                ByteBuffer midKey = getKey(mid);
                int comparison = ByteBuffers.compare(midKey, find, findLength);

                if (comparison < 0) {
                    min = mid + 1;
                    length = length - half - 1;
                } else {
                    length = half;
                }
            }

            return min;
        }

        Mutable(BranchPage page) {
            int n = page.getEntryCount();

            this.entries = new ArrayList<Entry>(n);

            for (int i = 0; i < n; i++) {
                ByteBuffer key = page.getKey(i);
                int pageNumber = page.getPageNumber(i);

                totalKeySize += key.remaining();

                Entry entry = new Entry(key, pageNumber);
                entries.add(entry);
            }
        }

        public void write(ByteBuffer buffer) {
            short n = (short) entries.size();
            buffer.putShort(n);

            short keyStart = (short) ((INDEX_ENTRY_SIZE * n) + 2);

            for (int i = 0; i < n; i++) {
                Entry entry = entries.get(i);

                buffer.putShort(keyStart);
                buffer.putInt(entry.pageNumber);

                keyStart += entry.key.remaining();
            }

            // Write a dummy tail entry so we know the total sizes
            buffer.putShort(keyStart);

            for (int i = 0; i < n; i++) {
                Entry entry = entries.get(i);
                buffer.put(entry.key.duplicate());
            }
        }

        public ByteBuffer getKey(int i) {
            return entries.get(i).key;
        }

        public int getEntryCount() {
            return entries.size();
        }

        public boolean walk(Transaction txn, ByteBuffer from, EntryListener listener) {
            int n = getEntryCount();
            int pos = lbound(from);
            while (pos < n) {
                Entry entry = entries.get(pos);

                int pageNumber = entry.pageNumber;

                Page childPage = txn.getPage(parent, pageNumber);

                boolean keepGoing = childPage.walk(txn, from, listener);
                if (!keepGoing) {
                    return false;
                }

                pos++;
            }

            return true;
        }

        public void updateLbound(Transaction txn, int pageNumber) {
            int n = getEntryCount();
            for (int i = 0; i < n; i++) {
                Entry entry = entries.get(i);
                if (entry.pageNumber != pageNumber) {
                    continue;
                }

                Page childPage = txn.getPage(parent, pageNumber);

                ByteBuffer lbound = childPage.getKeyLbound();
                entries.set(i, new Entry(lbound, pageNumber));
                return;
            }

            throw new IllegalStateException();
        }

        public void renumberChild(int oldPageNumber, int newPageNumber) {
            int n = getEntryCount();
            for (int i = 0; i < n; i++) {
                Entry entry = entries.get(i);
                if (entry.pageNumber != oldPageNumber) {
                    continue;
                }

                entries.set(i, new Entry(entry.key, newPageNumber));
                return;
            }

            throw new IllegalStateException();
        }

        public ByteBuffer getKeyLbound() {
            return getKey(0);
        }

        public boolean isDirty() {
            // TODO: Check for no-net-change?
            return true;
        }

        public int getSerializedSize() {
            short n = (short) entries.size();
            return ((INDEX_ENTRY_SIZE * n) + 2 + totalKeySize);
        }

        public void dump(PrintStream os) {
            os.println("BranchPage (dirty): count=" + getEntryCount());
            for (int i = 0; i < getEntryCount(); i++) {
                os.println("\t" + ByteBuffers.toHex(getKey(i)) + " => " + getPageNumber(i));
            }
        }

    }

    static class Entry {
        final ByteBuffer key;
        final int pageNumber;

        public Entry(ByteBuffer key, int pageNumber) {
            this.key = key;
            this.pageNumber = pageNumber;
        }
    }

    private int lbound(ByteBuffer find) {
        assert mutable == null;

        int findLength = find.remaining();

        int min = 0;

        int length = getEntryCount();
        while (length > 0) {
            int half = length >> 1;
            int mid = min + half;

            ByteBuffer midKey = getKey(mid);
            int comparison = ByteBuffers.compare(midKey, find, findLength);

            if (comparison < 0) {
                min = mid + 1;
                length = length - half - 1;
            } else {
                length = half;
            }
        }

        return min;
    }

    @Override
    public boolean walk(Transaction txn, ByteBuffer from, EntryListener listener) {
        if (mutable != null) {
            return mutable.walk(txn, from, listener);
        }

        int n = getEntryCount();
        int pos = lbound(from);
        while (pos < n) {
            int pageNumber = getPageNumber(pos);

            Page page = txn.getPage(parent, pageNumber);
            boolean keepGoing = page.walk(txn, from, listener);
            if (!keepGoing) {
                return false;
            }

            pos++;
        }

        return true;
    }

    private int getEntryCount() {
        assert mutable == null;
        return buffer.getShort(2);
    }

    private ByteBuffer getKey(int i) {
        assert mutable == null;

        ByteBuffer ret = buffer.duplicate();
        int offset = (i * INDEX_ENTRY_SIZE);
        int start = ret.getShort(offset);
        int end = ret.getShort(offset + INDEX_ENTRY_SIZE);

        ret.position(start);
        ret.limit(end);

        return ret;
    }

    private int getPageNumber(int i) {
        assert mutable == null;

        int offset = (i * INDEX_ENTRY_SIZE) + 2;
        int page = buffer.getInt(offset);
        return page;
    }

    Mutable getMutable() {
        if (mutable == null) {
            mutable = new Mutable(this);
        }
        return mutable;
    }

    @Override
    public void doAction(Transaction txn, KvAction action, ByteBuffer key, ByteBuffer value) {
        int pos = lbound(key);
        int pageNumber = getPageNumber(pos);

        Page childPage = txn.getPage(parent, pageNumber);

        ByteBuffer oldLbound = childPage.getKeyLbound();

        childPage.doAction(txn, action, key, value);

        ByteBuffer newLbound = childPage.getKeyLbound();

        if (!oldLbound.equals(newLbound)) {
            getMutable().updateLbound(txn, pageNumber);
        }
    }

    @Override
    public ByteBuffer getKeyLbound() {
        if (mutable != null) {
            return mutable.getKeyLbound();
        }

        return getKey(0);
    }

    public void renumberChild(int oldPageNumber, int newPageNumber) {
        getMutable().renumberChild(oldPageNumber, newPageNumber);
    }

    @Override
    public void write(ByteBuffer dest) {
        if (mutable == null) {
            throw new IllegalStateException();
        }

        mutable.write(dest);
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
        if (mutable == null) {
            throw new IllegalStateException();
        }

        return mutable.getSerializedSize();
    }

    @Override
    public byte getPageType() {
        return PAGE_TYPE;
    }

    @Override
    public void dump(PrintStream os) {
        if (mutable != null) {
            mutable.dump(os);
            return;
        }
        os.println("BranchPage: count=" + getEntryCount());
        for (int i = 0; i < getEntryCount(); i++) {
            os.println("\t" + ByteBuffers.toHex(getKey(i)) + " => " + getPageNumber(i));
        }
    }
}
