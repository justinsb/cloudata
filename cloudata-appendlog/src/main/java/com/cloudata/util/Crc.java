package com.cloudata.util;

import java.nio.ByteBuffer;

import org.apache.hadoop.util.PureJavaCrc32C;

public class Crc {

    public static void update(PureJavaCrc32C crc, ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray()) {
            crc.update(buffer.array(), buffer.arrayOffset() + offset, length);
        } else {
            // TODO: Add overload to PureJavaCrc32C if this pops up in perf trace
            byte[] array = new byte[Math.min(length, 4096)];

            ByteBuffer b = buffer.duplicate();
            b.position(offset);

            while (length > 0) {
                int n = Math.min(array.length, length);
                b.get(array, 0, n);

                crc.update(array, 0, n);

                length -= n;
            }
        }
    }

}
