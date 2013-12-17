package com.cloudata.keyvalue.values;

import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;

import com.cloudata.keyvalue.btree.ByteBuffers;
import com.cloudata.keyvalue.btree.operation.Value;
import com.cloudata.util.ByteBufferInputStream;
import com.google.common.base.Charsets;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class JsonValue extends Value {
    public JsonValue(ByteBuffer value) {
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
    public JsonValue clone() {
        return new JsonValue(ByteBuffers.clone(buffer));
    }

    @Override
    public long asLong() {
        JsonElement json = asJson();
        if (json.isJsonPrimitive()) {
            try {
                return json.getAsLong();
            } catch (Exception e) {
                // Can't get as long
                throw new IllegalArgumentException();
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    private JsonElement asJson() {
        // TODO: Cache
        ByteBufferInputStream is = new ByteBufferInputStream(asBytes());
        Reader reader = new InputStreamReader(is, Charsets.UTF_8);
        JsonElement json = new JsonParser().parse(reader);
        return json;
    }

    @Override
    public ByteBuffer asJsonString() {
        ByteBuffer b = buffer.asReadOnlyBuffer();
        b.position(b.position() + 1);
        return b;
    }
}
