package com.cloudata.keyvalue.btree;

import java.nio.ByteBuffer;

public class ByteBuffers {

    public static int compare(ByteBuffer l, ByteBuffer r) {
        int n = Math.min(l.remaining(), r.remaining());

        int lPos = l.position();
        int rPos = r.position();

        int end = lPos + n;
        while (lPos < end) {
            int comparison = compare(l.get(lPos), r.get(rPos));
            if (comparison != 0) {
                return comparison;
            }
            lPos++;
            rPos++;
        }

        return Integer.compare(l.remaining(), r.remaining());
    }

    public static int compare(ByteBuffer l, ByteBuffer r, int length) {
        int n = Math.min(l.remaining(), r.remaining());

        int lPos = l.position();
        int rPos = r.position();

        int end = lPos + n;
        while (lPos < end) {
            int comparison = compare(l.get(lPos), r.get(rPos));
            if (comparison != 0) {
                return comparison;
            }
            lPos++;
            rPos++;
        }

        if (length <= n) {
            return 0;
        } else {
            // One (or both) of the byte-strings was shorter than the requested length
            return Integer.compare(l.remaining(), r.remaining());
        }
    }

    private final static int compare(byte l, byte r) {
        // Compare as unsigned
        int il = l & 0xff;
        int ir = r & 0xff;

        return Integer.compare(il, ir);
    }
}
