package com.cloudata.keyvalue.redis;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public class Codec {
    public static final char CR = '\r';
    public static final char LF = '\n';

    static class Cache {
        private static final int cacheLow = -255;
        private static final int cacheHigh = 255;
        private static byte[][] cache;

        static {
            cache = new byte[1 + cacheHigh - cacheLow][];
            for (int i = 0; i < cache.length; i++) {
                long v = cacheLow + i;
                cache[i] = Long.toString(v).getBytes();
            }
        }
    }

    public static void writeLong(ByteBuf out, long value) {
        if (value >= Cache.cacheLow && value <= Cache.cacheHigh) {
            out.writeBytes(Cache.cache[(int) (value - Cache.cacheLow)]);
        } else {
            // TODO: Can we do much better (avoid the array)? I don't think we can...
            StringBuilder sb = new StringBuilder(20);
            sb.append(value);
            for (int i = 0; i < sb.length(); i++) {
                char c = sb.charAt(i);
                assert c == '-' || (c >= '0' && c <= '9');
                out.writeByte(c);
            }
        }
    }

    public static long readLongWithCrlf(ByteBuf buf) throws IOException {
        long v = 0;
        boolean negative = false;
        int read = buf.readByte();
        if (read == '-') {
            negative = true;
            read = buf.readByte();
        }
        do {
            if (read == CR) {
                if (buf.readByte() == LF) {
                    break;
                }
            }
            int value = read - '0';
            if (value >= 0 && value < 10) {
                v *= 10;
                v += value;
            } else {
                throw new IOException("Invalid character in integer");
            }
            read = buf.readByte();
        } while (true);

        if (negative) {
            v = -v;
        }
        return v;
    }
}
