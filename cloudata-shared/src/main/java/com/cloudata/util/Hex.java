package com.cloudata.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.ByteIterator;

public class Hex {

    static final char[] HEX = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    public static String toHex(ByteString key) {
        StringBuilder sb = new StringBuilder();

        ByteIterator iterator = key.iterator();
        while (iterator.hasNext()) {
            int v = iterator.nextByte() & 0xff;
            sb.append(HEX[v >> 4]);
            sb.append(HEX[v & 0xf]);
        }

        return sb.toString();
    }

    public static String forDebug(ByteString key) {
        if (key == null) {
            return "(null)";
        }
        return toHex(key);
    }

}
