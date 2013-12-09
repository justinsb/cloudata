package com.cloudata.keyvalue.btree;

import java.io.PrintStream;
import java.nio.ByteBuffer;

public abstract class Page {
    protected final Page parent;
    protected final ByteBuffer buffer;
    private final int originalPageNumber;

    protected Page(Page parent, int pageNumber, ByteBuffer buffer) {
        this.parent = parent;
        this.originalPageNumber = pageNumber;
        this.buffer = buffer;

        assert this.buffer.position() == 0;
    }

    public abstract boolean walk(Transaction txn, ByteBuffer from, EntryListener listener);

    public abstract void insert(Transaction txn, ByteBuffer key, ByteBuffer value);

    public abstract ByteBuffer getKeyLbound();

    public int getPageNumber() {
        return originalPageNumber;
    }

    public Page getParent() {
        return parent;
    }

    public abstract void write(ByteBuffer dest);

    public abstract boolean isDirty();

    public abstract int getSerializedSize();

    public abstract byte getPageType();

    public abstract void dump(PrintStream os);
}
