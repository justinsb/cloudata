package com.cloudata.btree;

import java.nio.ByteBuffer;

import com.cloudata.values.Value;

public class GetEntryListener implements EntryListener {
    final ByteBuffer findKey;
    Value foundValue;

    public GetEntryListener(ByteBuffer findKey) {
        this.findKey = findKey;
    }

    @Override
    public boolean found(ByteBuffer key, Value value) {
        // log.debug("Found {}={}", ByteBuffers.toHex(key), ByteBuffers.toHex(value));
        if (key.equals(findKey)) {
            foundValue = value;
        }
        // No more
        return false;
    }
}
