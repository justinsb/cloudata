package com.cloudata.appendlog.log;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class LogMessageIterable implements Iterable<LogMessage> {

    public static final LogMessageIterable EMPTY = new LogMessageIterable(ByteBuffer.allocate(0));
    final ByteBuffer buffer;

    public LogMessageIterable(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public Iterator<LogMessage> iterator() {
        return new LogMessageIterator(buffer);
    }

}
