package com.cloudata.appendlog.log;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogMessageIterator implements Iterator<LogMessage> {
    private static final Logger log = LoggerFactory.getLogger(LogMessageIterator.class);

    ByteBuffer data;
    LogMessage next;

    public LogMessageIterator(ByteBuffer data) {
        this.data = data.duplicate();
    }

    @Override
    public boolean hasNext() {
        if (next == null) {
            next = LogMessage.read(data);

            log.debug("Next entry: {}", next);
        }
        return next != null;
    }

    @Override
    public LogMessage next() {
        LogMessage ret = this.next;
        this.next = null;
        return ret;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}