package com.cloudata.keyvalue.btree;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.KeyValueProto.KvAction;

public class LeafPage extends Page {
    private static final Logger log = LoggerFactory.getLogger(LeafPage.class);

    public static final byte PAGE_TYPE = 'L';

    private static final int INDEX_ENTRY_SIZE = 4;

    Mutable mutable;

    final boolean uniqueKeys;

    public LeafPage(Page parent, int pageNumber, ByteBuffer buffer, boolean uniqueKeys) {
        super(parent, pageNumber, buffer);
        this.uniqueKeys = uniqueKeys;
    }

    static class Mutable {
        final boolean uniqueKeys;
        final List<Entry> entries;
        int totalKeySize;
        int totalValueSize;

        int lbound(ByteBuffer find) {
            int min = 0;

            int length = getEntryCount();
            while (length > 0) {
                int half = length >> 1;
                int mid = min + half;

                ByteBuffer midKey = getKey(mid);
                // log.debug("comparison (dirty): @{} = {}", mid, midKey);
                int comparison = ByteBuffers.compare(midKey, find);
                // log.debug("comparison (dirty): {} vs {}: {}", ByteBuffers.toHex(midKey), ByteBuffers.toHex(find),
                // comparison);

                if (comparison < 0) {
                    min = mid + 1;
                    length = length - half - 1;
                } else {
                    length = half;
                }
            }

            return min;
        }

        boolean doAction(KvAction action, ByteBuffer key, ByteBuffer value) {
            boolean changed = false;

            int position = lbound(key);

            switch (action) {
            case SET:
                Entry newEntry = new Entry(key, value);
                if (uniqueKeys) {
                    if (position < entries.size()) {
                        ByteBuffer midKey = getKey(position);
                        int comparison = ByteBuffers.compare(midKey, key);
                        if (comparison == 0) {
                            ByteBuffer oldValue = getValue(position);

                            entries.set(position, newEntry);

                            totalValueSize += value.remaining() - oldValue.remaining();
                            changed = true;
                        }
                    }
                }

                if (!changed) {
                    entries.add(position, newEntry);

                    totalKeySize += key.remaining();
                    totalValueSize += value.remaining();
                }

                break;

            case DELETE:
                if (uniqueKeys) {
                    if (position < entries.size()) {
                        ByteBuffer midKey = getKey(position);
                        int comparison = ByteBuffers.compare(midKey, key);
                        if (comparison == 0) {
                            ByteBuffer oldValue = getValue(position);

                            entries.remove(position);

                            totalKeySize -= key.remaining();
                            totalValueSize -= oldValue.remaining();
                            changed = true;
                            log.info("Deleted entry @{}", position);
                        } else {
                            log.info("Key not found in delete");
                        }
                    }
                } else {
                    // TODO: Support specifying entry by key & value
                    throw new UnsupportedOperationException();
                }
                break;

            default:
                throw new UnsupportedOperationException();
            }

            return changed;
        }

        Mutable(LeafPage page) {
            this.uniqueKeys = page.uniqueKeys;
            int n = page.getEntryCount();

            this.entries = new ArrayList<Entry>(n);

            for (int i = 0; i < n; i++) {
                ByteBuffer key = page.getKey(i);
                ByteBuffer value = page.getValue(i);
                totalKeySize += key.remaining();
                totalValueSize += value.remaining();

                Entry entry = new Entry(key, value);
                entries.add(entry);
            }
        }

        public int getSerializedSize() {
            short n = (short) entries.size();
            return 2 + (INDEX_ENTRY_SIZE * (n + 1)) + totalKeySize + totalValueSize;
        }

        public void write(ByteBuffer buffer) {
            assert buffer.remaining() == getSerializedSize();

            short n = (short) entries.size();
            buffer.putShort(n);

            short keyStart = (short) (2 + (INDEX_ENTRY_SIZE * (n + 1)));
            short valueStart = (short) (keyStart + totalKeySize);

            for (int i = 0; i < n; i++) {
                buffer.putShort(keyStart);
                buffer.putShort(valueStart);

                Entry entry = entries.get(i);
                keyStart += entry.key.remaining();
                valueStart += entry.value.remaining();
            }

            // Write a dummy tail entry so we know the total sizes
            // TODO: We can't do this if we want to use this for overflow values (>64KB)
            buffer.putShort(keyStart);
            buffer.putShort(valueStart);

            for (int i = 0; i < n; i++) {
                Entry entry = entries.get(i);
                buffer.put(entry.key.duplicate());
            }

            for (int i = 0; i < n; i++) {
                Entry entry = entries.get(i);
                buffer.put(entry.value.duplicate());
            }
        }

        public ByteBuffer getKey(int i) {
            return entries.get(i).key;
        }

        public ByteBuffer getValue(int i) {
            return entries.get(i).value;
        }

        public int getEntryCount() {
            return entries.size();
        }

        public boolean walk(Transaction txn, ByteBuffer from, EntryListener listener) {
            int n = getEntryCount();
            int pos = lbound(from);
            while (pos < n) {
                ByteBuffer key = getKey(pos);
                ByteBuffer value = getValue(pos);

                boolean keepGoing = listener.found(key.duplicate(), value.duplicate());
                if (!keepGoing) {
                    return false;
                }

                pos++;
            }

            return true;
        }

        public ByteBuffer getKeyLbound() {
            return getKey(0);
        }

        public boolean isDirty() {
            // TODO: Check for no-net-change?
            return true;
        }

        public void dump(PrintStream os) {
            os.println("LeafPage (dirty): count=" + getEntryCount());
            for (int i = 0; i < getEntryCount(); i++) {
                os.println("\t" + ByteBuffers.toHex(getKey(i)) + " => " + ByteBuffers.toHex(getValue(i)));
            }
        }

    }

    static class Entry {
        ByteBuffer key;
        ByteBuffer value;

        public Entry(ByteBuffer key, ByteBuffer value) {
            this.key = key;
            this.value = value;
        }
    }

    private int lbound(ByteBuffer find) {
        assert mutable == null;

        int min = 0;

        int length = getEntryCount();
        while (length > 0) {
            int half = length >> 1;
            int mid = min + half;

            ByteBuffer midKey = getKey(mid);
            // log.debug("comparison: @{} = {}", mid, midKey);
            int comparison = ByteBuffers.compare(midKey, find);

            // log.debug("comparison: {} vs {}: {}", ByteBuffers.toHex(midKey), ByteBuffers.toHex(find), comparison);

            if (comparison < 0) {
                min = mid + 1;
                length = length - half - 1;
            } else {
                length = half;
            }
        }

        // log.debug("lbound for {} is: {}", find, min);

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
            ByteBuffer key = getKey(pos);
            ByteBuffer value = getValue(pos);

            boolean keepGoing = listener.found(key, value);
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

    private ByteBuffer getValue(int i) {
        assert mutable == null;

        ByteBuffer ret = buffer.duplicate();
        int offset = 2 + (i * INDEX_ENTRY_SIZE) + 2;
        int start = ret.getShort(offset);
        int end = ret.getShort(offset + INDEX_ENTRY_SIZE);

        ret.position(start);
        ret.limit(end);

        return ret;
    }

    Mutable getMutable() {
        if (mutable == null) {
            mutable = new Mutable(this);
        }
        return mutable;
    }

    @Override
    public void doAction(Transaction txn, KvAction action, ByteBuffer key, ByteBuffer value) {
        getMutable().doAction(action, key, value);
    }

    @Override
    public ByteBuffer getKeyLbound() {
        if (mutable != null) {
            return mutable.getKeyLbound();
        }
        return getKey(0);
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

    public static LeafPage createNew(Page parent, int pageNumber, boolean uniqueKeys) {
        // TODO: Reuse a shared buffer?
        ByteBuffer empty = ByteBuffer.allocate(6);
        empty.putShort((short) 0);
        empty.putShort((short) 0);
        empty.putShort((short) 0);

        empty.flip();

        return new LeafPage(parent, pageNumber, empty, uniqueKeys);
    }

    @Override
    public String toString() {
        int count;
        if (mutable != null) {
            count = mutable.getEntryCount();
        } else {
            count = getEntryCount();
        }
        return "LeafPage [entryCount=" + count + "]";
    }

    @Override
    public void dump(PrintStream os) {
        if (mutable != null) {
            mutable.dump(os);
            return;
        }
        os.println("LeafPage: count=" + getEntryCount());
        for (int i = 0; i < getEntryCount(); i++) {
            os.println("\t" + ByteBuffers.toHex(getKey(i)) + " => " + ByteBuffers.toHex(getValue(i)));
        }
    }
}
