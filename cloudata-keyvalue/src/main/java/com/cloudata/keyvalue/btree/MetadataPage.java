package com.cloudata.keyvalue.btree;

import java.nio.ByteBuffer;

public class MetadataPage {

    final ByteBuffer buffer;
    final int offset;

    public MetadataPage(ByteBuffer buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
    }

    public int getRoot() {
        return buffer.getInt(offset);
    }

    public static void create(ByteBuffer mmap, int rootPage) {
        mmap.putInt(rootPage);
    }
}
