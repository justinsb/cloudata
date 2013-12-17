package com.cloudata.btree;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.List;

import com.cloudata.btree.operation.RowOperation;

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

    public abstract <V> void doAction(Transaction txn, ByteBuffer key, RowOperation<V> operation);

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

    public abstract List<Page> split(WriteTransaction txn);

    public abstract boolean shouldSplit();

    public String dump() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        dump(ps);
        return new String(baos.toByteArray());
    }

}
