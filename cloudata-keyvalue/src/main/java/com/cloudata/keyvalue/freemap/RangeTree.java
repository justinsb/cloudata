package com.cloudata.keyvalue.freemap;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.google.common.collect.Sets;

public class RangeTree {
    static class Range {
        int start;
        int end;

        public Range(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }

    /**
     * Compares the two ranges naturally, but returns 0 if they overlap
     */
    static class OverlapComparator implements Comparator<Range> {

        public static final OverlapComparator INSTANCE = new OverlapComparator();

        @Override
        public int compare(Range l, Range r) {
            if (l.start < r.start) {
                if (l.end > r.start) {
                    return 0;
                }
                return -1;
            }

            if (l.start > r.start) {
                if (l.start < r.end) {
                    return 0;
                }
                return 1;
            }

            return 0;
        }
    }

    private static final short VERSION_1 = 1;

    private static final int HEADER_SIZE = 8;

    final TreeSet<Range> tree;
    final AllocationStrategy allocationStrategy = new FirstFitAllocationStrategy(false);

    public RangeTree() {
        this.tree = Sets.newTreeSet(OverlapComparator.INSTANCE);
    }

    public int allocate(int size) {
        synchronized (tree) {
            int start = allocationStrategy.allocate(size);
            if (start < 0) {
                return start;
            }

            remove(start, size);
            return start;
        }
    }

    public void release(int start, int size) {
        synchronized (tree) {
            add(start, size);
        }
    }

    public void releaseAll(List<SpaceMapEntry> entries) {
        synchronized (tree) {
            for (SpaceMapEntry freed : entries) {
                add(freed.start, freed.length);
            }
        }
    }

    public void replayAllocate(int start, int size) {
        synchronized (tree) {
            remove(start, size);
        }
    }

    private void add(int start, int size) {
        int end = start + size;
        Range probe = new Range(start, end);

        Range before = tree.floor(probe);
        Range after = tree.ceiling(probe);

        if (before != null && before.start <= start && before.end >= end) {
            throw new IllegalStateException();
        }

        assert before == null || OverlapComparator.INSTANCE.compare(before, probe) != 0;

        boolean mergeBefore = before != null && before.end == start;
        boolean mergeAfter = after != null && after.start == end;

        if (mergeBefore && mergeAfter) {
            // We can change either one of them to be the merged value, and the order will be maintained
            tree.remove(before);

            after.start = before.start;
        } else if (mergeBefore) {
            before.end = end;
        } else if (mergeAfter) {
            after.start = start;
        } else {
            tree.add(probe);
        }
    }

    private void remove(int start, int size) {
        int end = start + size;
        Range probe = new Range(start, end);

        Range existing = tree.floor(probe);

        if (existing == null) {
            throw new IllegalStateException();
        }

        boolean extraAtStart = existing.start != start;
        boolean extraAtEnd = existing.end != end;

        if (extraAtStart && extraAtEnd) {
            existing.end = start;

            Range right = new Range(end, existing.end);
            tree.add(right);
        } else if (extraAtStart) {
            existing.end = start;
        } else if (extraAtEnd) {
            existing.start = end;
        } else {
            tree.remove(existing);
        }
    }

    abstract class AllocationStrategy {

        abstract int allocate(int size);
    }

    class FirstFitAllocationStrategy extends AllocationStrategy {
        int cursor;

        final boolean rotate;

        public FirstFitAllocationStrategy(boolean rotate) {
            this.rotate = rotate;
        }

        @Override
        int allocate(int size) {
            return allocate(cursor, size);
        }

        private int allocate(int start, int size) {
            Range probe = new Range(start, start + size);

            NavigableSet<Range> next;
            if (rotate) {
                next = tree.tailSet(probe, true);
            } else {
                next = tree;
            }
            Iterator<Range> it = next.iterator();
            while (it.hasNext()) {
                Range range = it.next();
                int end = range.start + size;
                if (end <= range.end) {
                    cursor = end;
                    return range.start;
                }
            }

            if (rotate) {
                if (start == 0) {
                    return -1;
                }
                cursor = 0;
                return allocate(0, size);
            } else {
                return -1;
            }
        }
    }

    int getSerializedSize() {
        synchronized (tree) {
            return HEADER_SIZE + tree.size() * 8;
        }
    }

    void write(ByteBuffer dest) {
        synchronized (tree) {
            dest.putShort(VERSION_1);
            dest.putShort((short) 0);
            dest.putInt(tree.size());
            Iterator<Range> it = tree.iterator();
            while (it.hasNext()) {
                Range range = it.next();
                dest.putInt(range.start);
                dest.putInt(range.end);
            }
        }
    }

    public static RangeTree deserialize(ByteBuffer buffer) {
        ByteBuffer read = buffer.duplicate();

        RangeTree rangeTree = new RangeTree();

        short version = read.getShort();
        if (version != VERSION_1) {
            throw new IllegalStateException();
        }

        short pad = read.getShort();
        int n = read.getInt();
        for (int i = 0; i < n; i++) {
            int start = read.getInt();
            int end = read.getInt();

            rangeTree.add(start, end - start);
        }

        return rangeTree;
    }

}
