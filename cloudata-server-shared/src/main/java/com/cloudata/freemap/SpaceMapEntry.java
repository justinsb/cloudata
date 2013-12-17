package com.cloudata.freemap;

public class SpaceMapEntry implements Comparable<SpaceMapEntry> {
    public final int start;
    public final int length;

    public SpaceMapEntry(int start, int length) {
        this.start = start;
        this.length = length;
    }

    @Override
    public int compareTo(SpaceMapEntry r) {
        SpaceMapEntry l = this;
        int v = Integer.compare(l.start, r.start);
        if (v == 0) {
            v = Integer.compare(l.length, r.length);
        }
        return v;
    }

    public int getPageId() {
        return start;
    }

    @Override
    public String toString() {
        return "SpaceMapEntry [start=" + start + ", length=" + length + "]";
    }

}
