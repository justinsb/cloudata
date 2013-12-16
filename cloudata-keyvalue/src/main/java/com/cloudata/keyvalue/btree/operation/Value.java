package com.cloudata.keyvalue.btree.operation;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.btree.ByteBuffers;
import com.google.protobuf.ByteString;

public abstract class Value implements Cloneable {
    public static final byte FORMAT_RAW = 0;
    public static final byte FORMAT_INT64 = 1;

    static final ByteString PREFIX_RAW = ByteString.copyFrom(new byte[] { FORMAT_RAW });
    static final ByteString PREFIX_INT64 = ByteString.copyFrom(new byte[] { FORMAT_INT64 });

    final ByteBuffer buffer;

    public Value(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public abstract Value clone();

    static class Int64Value extends Value {
        public Int64Value(ByteBuffer buffer) {
            super(buffer);
        }

        @Override
        public ByteBuffer asBytes() {
            long v = asLong();
            return ByteBuffer.wrap(Long.toString(v).getBytes());
        }

        @Override
        public int sizeAsBytes() {
            // This isn't fast...
            throw new UnsupportedOperationException();
        }

        @Override
        public long asLong() {
            return buffer.getLong(buffer.position() + 1);
        }

        @Override
        public Int64Value clone() {
            return new Int64Value(ByteBuffers.clone(buffer));
        }

    }

    static class RawBytesValue extends Value {
        public RawBytesValue(ByteBuffer value) {
            super(value);
        }

        @Override
        public ByteBuffer asBytes() {
            ByteBuffer b = buffer.duplicate();
            b.position(b.position() + 1);
            return b;
        }

        @Override
        public int sizeAsBytes() {
            return buffer.remaining() - 1;
        }

        @Override
        public RawBytesValue clone() {
            return new RawBytesValue(ByteBuffers.clone(buffer));
        }

        @Override
        public long asLong() {
            ByteBuffer b = buffer.duplicate();
            b.position(b.position() + 1);
            long v = ByteBuffers.parseLong(b);
            return v;
        }

    }

    public static Value fromRawBytes(byte[] data) {
        return fromRawBytes(ByteString.copyFrom(data));
    }

    public static Value fromRawBytes(ByteString b) {
        return new RawBytesValue(PREFIX_RAW.concat(b).asReadOnlyByteBuffer());
    }

    public Value concat(ByteString appendValue) {
        ByteBuffer old = asBytes();

        int n = old.remaining() + appendValue.size();

        ByteBuffer appended = ByteBuffer.allocate(1 + n);
        appended.put(FORMAT_RAW);
        appended.put(old.duplicate());
        appended.put(appendValue.asReadOnlyByteBuffer());
        appended.flip();

        return new RawBytesValue(appended);
    }

    public static Value fromLong(long v) {
        ByteBuffer b = ByteBuffer.allocate(9);
        b.put(FORMAT_INT64);
        b.putLong(v);
        b.flip();

        return new Int64Value(b);
    }

    public abstract ByteBuffer asBytes();

    public abstract long asLong();

    public abstract int sizeAsBytes();

    public static Value deserialize(ByteBuffer buffer) {
        byte type = buffer.get(buffer.position());

        switch (type) {
        case FORMAT_RAW: {
            return new RawBytesValue(buffer);
        }

        case FORMAT_INT64: {
            return new Int64Value(buffer);
        }

        default:
            throw new IllegalStateException();
        }
    }

    public ByteBuffer serialize() {
        return buffer.duplicate();
    }

}
