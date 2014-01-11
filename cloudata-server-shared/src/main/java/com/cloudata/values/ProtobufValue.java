package com.cloudata.values;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.cloudata.btree.ByteBuffers;
import com.cloudata.objectstore.ByteBufferOutputStream;
import com.cloudata.util.ByteBufferInputStream;
import com.cloudata.util.Hex;
import com.google.gson.JsonObject;
import com.google.protobuf.Message;

public class ProtobufValue extends Value {
    public ProtobufValue(ByteBuffer value) {
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
    public ProtobufValue clone() {
        return new ProtobufValue(ByteBuffers.clone(buffer));
    }

    @Override
    public long asLong() {
        // Not (really) possible
        throw new IllegalArgumentException();
    }

    @Override
    public JsonObject asJsonObject() {
        // This is fairly easily implemented!!
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer asJsonString() {
        // This is fairly easily implemented!!
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "ProtobufValue [" + Hex.forDebug(asBytes()) + "]";
    }

    @Override
    public void asProtobuf(Message.Builder builder) throws IOException {
        ByteBuffer bytes = asBytes();
        builder.mergeFrom(new ByteBufferInputStream(bytes));
    }

    public static ProtobufValue build(Message m) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(1 + m.getSerializedSize());
        b.put(FORMAT_PROTOBUF);
        m.writeTo(new ByteBufferOutputStream(b));
        b.flip();

        return new ProtobufValue(b);
    }

}