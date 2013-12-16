package com.cloudata.keyvalue.btree;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.btree.operation.KeyOperation;
import com.cloudata.keyvalue.btree.operation.Value;
import com.cloudata.util.Hex;
import com.google.common.collect.Lists;
import com.google.common.primitives.Shorts;

/**
 * LeafPage stores a leaf of a btree
 * 
 * The data format looks like this:
 * 
 * short: # of entries
 * 
 * (short short)*: start position of key and start position of value
 * 
 * (short short): end position of last key and value
 * 
 * key data
 * 
 * value data
 * 
 * 
 * There is a special case format when the # of entries is 1. Then instead of storing the (short short) with the end
 * positions, we instead store an (int) with the length of the value. This allows for values > 32KB.
 */
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
        private static final int MAX_SIZE = Short.MAX_VALUE;

        final boolean uniqueKeys;
        final List<Entry> entries;
        int totalKeySize;
        int totalValueSize;

        int firstGTE(ByteBuffer find) {
            int n = getEntryCount();
            int pos;
            {
                int min = 0;

                int length = n;

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
                pos = min;
            }

            // Check this is the first >= find
            assert pos == n || ByteBuffers.compare(getKey(pos), find) >= 0;
            assert pos == 0 || ByteBuffers.compare(getKey(pos - 1), find) < 0;

            return pos;
        }

        <V> void doAction(ByteBuffer key, KeyOperation<V> operation) {
            int position = firstGTE(key);

            Value oldValue = null;
            ByteBuffer oldValueBuffer = null;
            if (uniqueKeys) {
                if (position < entries.size()) {
                    ByteBuffer positionKey = getKey(position);
                    int positionComparison = ByteBuffers.compare(positionKey, key);
                    if (positionComparison == 0) {
                        oldValueBuffer = getValue(position);
                        oldValue = Value.deserialize(oldValueBuffer);
                    }
                }
            } else {
                throw new UnsupportedOperationException();
            }

            Value newValue = operation.doAction(oldValue);

            if (newValue == oldValue) {
                // No change (either both null or no value change)
            } else if (newValue == null && oldValue != null) {
                // Delete entry
                entries.remove(position);

                totalKeySize -= key.remaining();
                totalValueSize -= oldValueBuffer.remaining();
                log.info("Deleted entry @{}", position);
            } else if (newValue != null && oldValue == null) {
                // Insert new entry
                ByteBuffer newValueBuffer = newValue.serialize();
                Entry newEntry = new Entry(key, newValueBuffer);
                entries.add(position, newEntry);

                totalKeySize += key.remaining();
                totalValueSize += newValueBuffer.remaining();
            } else {
                // Update value
                assert newValue != null;
                assert oldValue != null;

                ByteBuffer newValueBuffer = newValue.serialize();

                Entry newEntry = new Entry(key, newValueBuffer);
                entries.set(position, newEntry);

                totalValueSize += newValueBuffer.remaining() - oldValueBuffer.remaining();
            }

            // return newValue;
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

        List<Page> split(WriteTransaction transaction, LeafPage original) {
            List<Page> extraPages = Lists.newArrayList();

            int n = getEntryCount();

            int limit = this.getSerializedSize() / 2;
            limit = Math.min(MAX_SIZE, limit);

            int keep = 0;

            int currentSize = 0;
            LeafPage target = null;
            for (int i = 0; i < n; i++) {
                ByteBuffer key = getKey(i);
                ByteBuffer value = getValue(i);

                int entrySize = key.remaining() + value.remaining() + INDEX_ENTRY_SIZE;
                if (target != null && ((currentSize + entrySize) > limit)) {
                    target = null;
                    currentSize = 0;
                }

                if (target == null) {
                    if (i == 0) {
                        target = original;
                    } else {
                        int pageNumber = transaction.assignPageNumber();
                        target = LeafPage.createNew(original.parent, pageNumber, uniqueKeys);
                        extraPages.add(target);
                    }

                    currentSize = 2 + INDEX_ENTRY_SIZE;
                }

                if (target == original) {
                    keep = i;
                } else {
                    Mutable targetMutable = target.getMutable();
                    Entry entry = entries.get(i);
                    targetMutable.entries.add(entry);
                    targetMutable.totalKeySize += entry.key.remaining();
                    targetMutable.totalValueSize += entry.value.remaining();
                }

                currentSize += entrySize;
            }

            // Otherwise we shouldn't be here...
            if (extraPages.isEmpty()) {
                throw new IllegalStateException();
            }

            assert keep >= 0;
            for (int i = n - 1; i > keep; i--) {
                Entry removed = entries.remove(i);

                totalKeySize -= removed.key.remaining();
                totalValueSize -= removed.value.remaining();
            }

            return extraPages;
        }

        public int getSerializedSize() {
            // Notice that the alternate format still has the same header size :-)
            int n = entries.size();
            return 2 + (INDEX_ENTRY_SIZE * (n + 1)) + totalKeySize + totalValueSize;
        }

        public void write(ByteBuffer buffer) {
            assert buffer.remaining() == getSerializedSize();

            int n = entries.size();
            buffer.putShort((short) n);

            short keyStart = Shorts.checkedCast(2 + (INDEX_ENTRY_SIZE * (n + 1)));
            short valueStart = Shorts.checkedCast(keyStart + totalKeySize);

            for (int i = 0; i < n; i++) {
                buffer.putShort(keyStart);
                buffer.putShort(valueStart);

                Entry entry = entries.get(i);
                keyStart += entry.key.remaining();
                valueStart += entry.value.remaining();
            }

            if (n == 1) {
                // Special case: we write the value length to allow huge values
                assert totalValueSize == entries.get(0).value.remaining();
                buffer.putInt(totalValueSize);
            } else {
                // Write a dummy tail entry so we know the total sizes
                // TODO: We can't do this if we want to use this for overflow values (>64KB)
                buffer.putShort(keyStart);
                buffer.putShort(valueStart);
            }

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
            int pos = from != null ? firstGTE(from) : 0;
            while (pos < n) {
                ByteBuffer key = getKey(pos);
                ByteBuffer valueBuffer = getValue(pos);

                Value value = Value.deserialize(valueBuffer);

                boolean keepGoing = listener.found(key.duplicate(), value);
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
                os.println("\t" + Hex.forDebug(getKey(i)) + " => " + Hex.forDebug(getValue(i)));
            }
        }

        public boolean shouldSplit() {
            return entries.size() > 1 && getSerializedSize() > MAX_SIZE;
        }

        @Override
        public String toString() {
            ByteBuffer minKey = null;
            if (getEntryCount() != 0) {
                minKey = getKeyLbound();
            }
            return "LeafPage (mutable) [entryCount=" + getEntryCount() + ", minKey=" + Hex.forDebug(minKey) + "]";
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

    private int firstGTE(ByteBuffer find) {
        assert mutable == null;

        int n = getEntryCount();

        int pos;
        {
            int min = 0;

            int length = n;
            while (length > 0) {
                int half = length >> 1;
                int mid = min + half;

                ByteBuffer midKey = getKey(mid);
                // log.debug("comparison: @{} = {}", mid, midKey);
                int comparison = ByteBuffers.compare(midKey, find);

                // log.debug("comparison: {} vs {}: {}", ByteBuffers.toHex(midKey), ByteBuffers.toHex(find),
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

        return pos;
    }

    @Override
    public boolean walk(Transaction txn, ByteBuffer from, EntryListener listener) {
        if (mutable != null) {
            return mutable.walk(txn, from, listener);
        }

        int n = getEntryCount();
        int pos = from != null ? firstGTE(from) : 0;
        while (pos < n) {
            ByteBuffer key = getKey(pos);
            ByteBuffer value = getValue(pos);

            boolean keepGoing = listener.found(key, Value.deserialize(value));
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

        int n = getEntryCount();

        assert 0 <= i && i < n;

        ByteBuffer ret = buffer.duplicate();
        int start;
        int end;
        if (n == 1) {
            // Alternate format: the first key ends where the first value begins
            // TODO: Should we instead have a 'blob' page? Could mean less copying around of data..
            start = ret.getShort(2);
            end = ret.getShort(4);
        } else {
            int offset = 2 + (i * INDEX_ENTRY_SIZE);
            start = ret.getShort(offset);
            end = ret.getShort(offset + INDEX_ENTRY_SIZE);
        }
        ret.position(start);
        ret.limit(end);

        return ret;
    }

    private ByteBuffer getValue(int i) {
        assert mutable == null;

        int n = getEntryCount();
        assert 0 <= i && i < n;

        ByteBuffer ret = buffer.duplicate();
        int start;
        int end;
        if (n == 1) {
            // Alternate format: the value length is a 32 bit int
            start = ret.getShort(4);
            end = start + ret.getInt(6);
        } else {
            int offset = 2 + (i * INDEX_ENTRY_SIZE) + 2;
            start = ret.getShort(offset);
            end = ret.getShort(offset + INDEX_ENTRY_SIZE);
        }
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
    public <V> void doAction(Transaction txn, ByteBuffer key, KeyOperation<V> operation) {
        getMutable().doAction(key, operation);
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
        if (mutable != null) {
            return mutable.toString();
        }
        ByteBuffer minKey = null;
        if (getEntryCount() != 0) {
            minKey = getKeyLbound();
        }
        return "LeafPage [entryCount=" + getEntryCount() + ", minKey=" + Hex.forDebug(minKey) + "]";
    }

    @Override
    public void dump(PrintStream os) {
        if (mutable != null) {
            mutable.dump(os);
            return;
        }
        os.println("LeafPage: count=" + getEntryCount());
        for (int i = 0; i < getEntryCount(); i++) {
            os.println("\t" + Hex.forDebug(getKey(i)) + " => " + Hex.forDebug(getValue(i)));
        }
    }

    @Override
    public List<Page> split(WriteTransaction txn) {
        return mutable.split(txn, this);
    }

    @Override
    public boolean shouldSplit() {
        if (mutable == null) {
            return false;
        }
        return mutable.shouldSplit();
    }
}
