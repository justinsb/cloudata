package com.cloudata.keyvalue.btree.operation;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.btree.ByteBuffers;
import com.google.protobuf.ByteString;

public class Values {
    public static final byte FORMAT_RAW = 0;
    public static final byte FORMAT_INT64 = 1;

    static final ByteString PREFIX_RAW = ByteString.copyFrom(new byte[] { FORMAT_RAW });

    static final ByteString PREFIX_INT64 = ByteString.copyFrom(new byte[] { FORMAT_INT64 });

    public static ByteBuffer fromRawBytes(byte[] data) {
        return fromRawBytes(ByteString.copyFrom(data));
    }

    public static ByteBuffer fromRawBytes(ByteString b) {
        return PREFIX_RAW.concat(b).asReadOnlyByteBuffer();
    }

    // TODO: Fix this...
    public static ByteBuffer concat(ByteBuffer oldValue, ByteString appendValue) {
        ByteBuffer old = Values.asBytes(oldValue);

        int n = old.remaining() + appendValue.size();

        ByteBuffer appended = ByteBuffer.allocate(1 + n);
        appended.put(FORMAT_RAW);
        appended.put(old.duplicate());
        appended.put(appendValue.asReadOnlyByteBuffer());
        appended.flip();

        return appended;
    }

    public static ByteBuffer fromLong(long v) {
        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.put(FORMAT_INT64);
        buffer.putLong(v);
        buffer.flip();
        return buffer;
    }

    public static byte getType(ByteBuffer data) {
        return data.get(data.position());
    }

    public static ByteBuffer asBytes(ByteBuffer data) {
        switch (getType(data)) {
        case FORMAT_RAW: {
            ByteBuffer b = data.duplicate();
            b.position(b.position() + 1);
            return b;
        }

        case FORMAT_INT64: {
            long v = data.getLong(data.position() + 1);
            return ByteBuffer.wrap(Long.toString(v).getBytes());
        }

        default:
            throw new IllegalStateException();
        }
    }

    public static long asLong(ByteBuffer data) {
        switch (getType(data)) {
        case FORMAT_RAW: {
            ByteBuffer b = data.duplicate();
            b.position(b.position() + 1);
            long v = ByteBuffers.parseLong(b);
            return v;
        }

        case FORMAT_INT64:
            return data.getLong(data.position() + 1);

        default:
            throw new IllegalStateException();
        }
    }

    public static int sizeAsBytes(ByteBuffer data) {
        switch (getType(data)) {
        case FORMAT_RAW: {
            return data.remaining() - 1;
        }

        case FORMAT_INT64: {
            // This isn't fast...
            throw new UnsupportedOperationException();
        }

        default:
            throw new IllegalStateException();
        }
    }

}
