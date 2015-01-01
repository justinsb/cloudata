package com.cloudata.values;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.cloudata.btree.ByteBuffers;
import com.cloudata.util.Hex;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

public abstract class Value implements Cloneable {
    public static final byte FORMAT_RAW = 0;
    public static final byte FORMAT_INT64 = 1;
    public static final byte FORMAT_JSON = 2;
    public static final byte FORMAT_PROTOBUF = 3;
    public static final byte FORMAT_QUALIFIED_KEY = 4;

    static final ByteString PREFIX_RAW = ByteString.copyFrom(new byte[] { FORMAT_RAW });
    // static final ByteString PREFIX_INT64 = ByteString.copyFrom(new byte[] { FORMAT_INT64 });
    static final ByteString PREFIX_JSON = ByteString.copyFrom(new byte[] { FORMAT_JSON });
    static final ByteString PREFIX_QUALIFIED_KEY = ByteString.copyFrom(new byte[] { FORMAT_QUALIFIED_KEY });
    static final ByteString PREFIX_PROTOBUF = ByteString.copyFrom(new byte[] { FORMAT_PROTOBUF });

    protected final ByteBuffer buffer;

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

        @Override
        public ByteBuffer asJsonString() {
            throw new UnsupportedOperationException();
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

        @Override
        public ByteBuffer asJsonString() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "RawBytesValue [" + Hex.forDebug(asBytes()) + "]";
        }

    }

    public static Value fromRawBytes(byte[] data) {
        return fromRawBytes(ByteString.copyFrom(data));
    }

    public static Value fromRawBytes(ByteString b) {
        return new RawBytesValue(PREFIX_RAW.concat(b).asReadOnlyByteBuffer());
    }

    public static Value fromQualifiedKey(ByteString qualifiedPrimaryKey) {
        return new RawBytesValue(PREFIX_QUALIFIED_KEY.concat(qualifiedPrimaryKey).asReadOnlyByteBuffer());
    }

    public static Value fromJsonBytes(byte[] data) {
        return fromJsonBytes(ByteString.copyFrom(data));
    }

    public static Value fromJsonBytes(ByteString b) {
        return new JsonValue(PREFIX_JSON.concat(b).asReadOnlyByteBuffer());
    }

    public static Value fromProtobuf(Message msg) {
        return new ProtobufValue(PREFIX_PROTOBUF.concat(msg.toByteString()).asReadOnlyByteBuffer());
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

    public Value concat(ByteBuffer appendValue) {
        appendValue = appendValue.duplicate();

        ByteBuffer old = asBytes();

        int n = old.remaining() + appendValue.remaining();

        ByteBuffer appended = ByteBuffer.allocate(1 + n);
        appended.put(FORMAT_RAW);
        appended.put(old.duplicate());
        appended.put(appendValue);
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

    public abstract ByteBuffer asJsonString();

    public abstract int sizeAsBytes();

    public static Value deserialize(ByteString b) {
        return deserialize(b.asReadOnlyByteBuffer());
    }

    public static Value deserialize(ByteBuffer buffer) {
        byte type = buffer.get(buffer.position());

        switch (type) {
        case FORMAT_RAW: {
            return new RawBytesValue(buffer);
        }

        case FORMAT_JSON: {
            return new JsonValue(buffer);
        }

        case FORMAT_INT64: {
            return new Int64Value(buffer);
        }

        case FORMAT_PROTOBUF: {
            return new ProtobufValue(buffer);
        }

        case FORMAT_QUALIFIED_KEY: {
            return new QualifiedKeyValue(buffer);
        }

        default:
            throw new IllegalStateException();
        }
    }

    public ByteBuffer serialize() {
        return buffer.duplicate();
    }

    public ByteString serializeToByteString() {
        return ByteString.copyFrom(serialize());
    }

    public JsonObject asJsonObject() {
        throw new UnsupportedOperationException();
    }

    public void asProtobuf(Message.Builder builder) throws IOException {
        throw new IllegalArgumentException();
    }

}
