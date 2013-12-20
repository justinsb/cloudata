package com.cloudata.files.webdav;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpFetchRange {
    private static final Logger log = LoggerFactory.getLogger(HttpFetchRange.class);

    private final Long from;
    private final Long to;

    public HttpFetchRange(Long from, Long to) {
        this.from = from;
        this.to = to;
    }

    public static HttpFetchRange parse(String range) {
        if (range.startsWith("bytes=")) {
            String byteRange = range.substring(6);
            int dashIndex = byteRange.indexOf('-');
            if (dashIndex == -1) {
                throw new IllegalArgumentException("Cannot parse byte range: " + range);
            }

            String fromString = byteRange.substring(0, dashIndex);
            String toString = byteRange.substring(dashIndex + 1);

            Long from = null;
            if (!fromString.isEmpty()) {
                from = Long.parseLong(fromString);
            }

            Long to = null;
            if (!toString.isEmpty()) {
                to = Long.parseLong(toString);
            }
            return new HttpFetchRange(from, to);
        } else {
            throw new IllegalArgumentException("Don't know how to parse: " + range);
        }
    }

    public Long getFrom() {
        return from;
    }

    public Long getTo() {
        return to;
    }
}
