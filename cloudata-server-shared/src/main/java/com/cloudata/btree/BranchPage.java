package com.cloudata.btree;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.operation.RowOperation;
import com.cloudata.util.Hex;

public class BranchPage extends Page {
    private static final Logger log = LoggerFactory.getLogger(BranchPage.class);

    public static final byte PAGE_TYPE = 'B';

    private static final int INDEX_ENTRY_SIZE = 6;

    Mutable mutable;

    public BranchPage(Page parent, int pageNumber, ByteBuffer buffer) {
        super(parent, pageNumber, buffer);
    }

    static class Mutable {
        final List<Entry> entries;
        int totalKeySize;
        final Page page;

        private int findPos(ByteBuffer find) {
            // We want the first =, or if no match, the last LT

            // First, do a normal binary search to find the first >=
            int n = getEntryCount();
            int pos;
            {
                int min = 0;

                int length = n;
                while (length > 0) {
                    int half = length >> 1;
                    int mid = min + half;

                    ByteBuffer midKey = getKey(mid);
                    int comparison = ByteBuffers.compare(midKey, find);
                    // log.info("comparison: @{} {} vs {}: {}", mid, Hex.forDebug(midKey), Hex.forDebug(find),
                    // comparison);

                    if (comparison < 0) {
                        min = mid + 1;
                        length = length - half - 1;
                    } else {
                        length = half;
                    }
                }
                pos = min;
            }

            // Check this is the first >= find
            assert pos == n || ByteBuffers.compare(getKey(pos), find) >= 0;
            assert pos == 0 || ByteBuffers.compare(getKey(pos - 1), find) < 0;

            // Return = if matching, otherwise return the last lt
            if (n == pos) {
                return pos - 1;
            } else {
                int comparison = ByteBuffers.compare(getKey(pos), find);

                if (comparison == 0) {
                    return pos;
                } else {
                    assert comparison > 0;
                    return pos - 1;
                }
            }
        }

        Mutable(BranchPage page) {
            this.page = page;

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

            short keyStart = (short) (2 + (INDEX_ENTRY_SIZE * n) + 2);

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

        public int getPageNumber(int i) {
            return entries.get(i).pageNumber;
        }

        public int getEntryCount() {
            return entries.size();
        }

        public boolean walk(Transaction txn, ByteBuffer from, EntryListener listener) {
            int n = getEntryCount();
            int pos = from != null ? findPos(from) : 0;
            if (pos < 0) {
                pos = 0;
            }

            while (pos < n) {
                Entry entry = entries.get(pos);

                int pageNumber = entry.pageNumber;

                Page childPage = txn.getPage(page, pageNumber);

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

                ByteBuffer oldKey = entry.key;
                Page childPage = txn.getPage(page, pageNumber);

                ByteBuffer newKey = childPage.getKeyLbound();
                entries.set(i, new Entry(newKey, pageNumber));

                totalKeySize += newKey.remaining() - oldKey.remaining();

                return;
            }

            throw new IllegalStateException();
        }

        void setChildren(Page page) {
            if (!this.entries.isEmpty()) {
                throw new IllegalStateException();
            }

            ByteBuffer key = page.getKeyLbound();
            entries.add(new Entry(key, page.getPageNumber()));

            totalKeySize += key.remaining();
        }

        int getPagePosition(int oldPageNumber) {
            int n = getEntryCount();
            for (int i = 0; i < n; i++) {
                Entry entry = entries.get(i);
                if (entry.pageNumber != oldPageNumber) {
                    continue;
                }
                return i;
            }

            throw new IllegalStateException();
        }

        public List<Page> splitChild(WriteTransaction txn, int oldPageNumber, Page page) {
            int pos = getPagePosition(oldPageNumber);

            log.info("Splitting page: {}", page.dump());

            List<Page> extraPages = page.split(txn);

            log.info("  Split to: {}", page.dump());

            int i = pos + 1;
            for (Page extraPage : extraPages) {
                log.info("  Split to: {}", extraPage.dump());

                ByteBuffer key = extraPage.getKeyLbound();
                Entry newEntry = new Entry(key, extraPage.getPageNumber());
                entries.add(i, newEntry);
                totalKeySize += key.remaining();
                i++;
            }

            log.info("  After split: {}", this.dump());

            return extraPages;
        }

        public void renumberChild(int oldPageNumber, int newPageNumber) {
            int pos = getPagePosition(oldPageNumber);
            entries.set(pos, new Entry(getKey(pos), newPageNumber));
            // No key change, so no need to touch totalKeySize
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
            return 2 + (INDEX_ENTRY_SIZE * n) + 2 + totalKeySize;
        }

        public void dump(PrintStream os) {
            os.println("BranchPage (dirty): count=" + getEntryCount());
            for (int i = 0; i < getEntryCount(); i++) {
                os.println("\t" + Hex.forDebug(getKey(i)) + " => #" + getPageNumber(i));
            }
        }

        public String dump() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            dump(ps);
            return new String(baos.toByteArray());
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

    // private int ltBound(ByteBuffer find) {
    // assert mutable == null;
    //
    // int findLength = find.remaining();
    //
    // int min = 0;
    //
    // int length = getEntryCount();
    // while (length > 0) {
    // int half = length >> 1;
    // int mid = min + half;
    //
    // ByteBuffer midKey = getKey(mid);
    // int comparison = ByteBuffers.compare(midKey, find, findLength);
    //
    // if (comparison < 0) {
    // min = mid + 1;
    // length = length - half - 1;
    // } else {
    // length = half;
    // }
    // }
    //
    // return min;
    // }

    private int findPos(ByteBuffer find) {
        assert mutable == null;

        // We want the first =, or if no match, the last LT

        // First, do a normal binary search to find the first >=
        int n = getEntryCount();
        int pos;
        {
            int min = 0;

            int length = n;
            while (length > 0) {
                int half = length >> 1;
                int mid = min + half;

                ByteBuffer midKey = getKey(mid);
                int comparison = ByteBuffers.compare(midKey, find);
                // log.info("comparison: @{} {} vs {}: {}", mid, Hex.forDebug(midKey), Hex.forDebug(find), comparison);

                if (comparison < 0) {
                    min = mid + 1;
                    length = length - half - 1;
                } else {
                    length = half;
                }
            }
            pos = min;
        }

        // Check this is the first >= find
        assert pos == n || ByteBuffers.compare(getKey(pos), find) >= 0;
        assert pos == 0 || ByteBuffers.compare(getKey(pos - 1), find) < 0;

        // Return = if matching, otherwise return the last lt
        if (pos == n) {
            return pos - 1;
        } else {
            int comparison = ByteBuffers.compare(getKey(pos), find);

            if (comparison == 0) {
                return pos;
            } else {
                assert comparison > 0;
                return pos - 1;
            }
        }
    }

    @Override
    public boolean walk(Transaction txn, ByteBuffer from, EntryListener listener) {
        if (mutable != null) {
            return mutable.walk(txn, from, listener);
        }

        int n = getEntryCount();
        int pos = from != null ? findPos(from) : 0;
        if (pos < 0) {
            pos = 0;
        }
        while (pos < n) {
            int pageNumber = getPageNumber(pos);

            Page page = txn.getPage(this, pageNumber);
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
        return buffer.getShort(0);
    }

    private ByteBuffer getKey(int i) {
        assert mutable == null;

        ByteBuffer ret = buffer.duplicate();
        int offset = 2 + (i * INDEX_ENTRY_SIZE);
        int start = ret.getShort(offset);
        int end = ret.getShort(offset + INDEX_ENTRY_SIZE);

        ret.position(start);
        ret.limit(end);

        return ret;
    }

    private int getPageNumber(int i) {
        assert mutable == null;

        assert i >= 0;
        assert i < getEntryCount();

        int offset = 2 + (i * INDEX_ENTRY_SIZE) + 2;
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
    public <V> void doAction(Transaction txn, ByteBuffer key, RowOperation<V> operation) {
        int pos = findPos(key);
        if (pos < 0) {
            pos = 0;
        }

        int pageNumber = getPageNumber(pos);

        Page childPage = txn.getPage(this, pageNumber);

        ByteBuffer oldLbound = childPage.getKeyLbound();

        childPage.doAction(txn, key, operation);

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
            os.println("\t" + Hex.forDebug(getKey(i)) + " => #" + getPageNumber(i));
        }
    }

    public static BranchPage createNew(Page parent, int pageNumber, Page childPage) {
        ByteBuffer empty = ByteBuffer.allocate(6);
        empty.putShort((short) 0);
        empty.flip();

        BranchPage page = new BranchPage(parent, pageNumber, empty);
        page.getMutable().setChildren(childPage);

        return page;
    }

    public List<Page> splitChild(WriteTransaction txn, int oldPageNumber, Page page) {
        return getMutable().splitChild(txn, oldPageNumber, page);
    }

    @Override
    public List<Page> split(WriteTransaction txn) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean shouldSplit() {
        return false;
    }

    @Override
    public String toString() {
        int count;
        if (mutable != null) {
            count = mutable.getEntryCount();
        } else {
            count = getEntryCount();
        }
        return "BranchPage [entryCount=" + count + "]";
    }

}
