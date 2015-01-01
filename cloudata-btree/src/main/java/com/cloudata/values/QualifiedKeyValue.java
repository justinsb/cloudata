package com.cloudata.values;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.cloudata.btree.ByteBuffers;
import com.cloudata.btree.Keyspace;
import com.cloudata.util.Hex;
import com.cloudata.values.Value.RawBytesValue;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;

public class QualifiedKeyValue extends RawBytesValue {

    private ByteString unqualified;
    private Keyspace keyspace;

    public QualifiedKeyValue(ByteBuffer value) {
        super(value);
    }

    @Override
    public QualifiedKeyValue clone() {
        return new QualifiedKeyValue(ByteBuffers.clone(buffer));
    }

    @Override
    public String toString() {
        // TODO: Decode
        return "QualifiedKeyValue [" + Hex.forDebug(asBytes()) + "]";
    }

    public ByteString getUnqualified() {
        if (unqualified == null) {
            parse();
        }
        return unqualified;
    }

    public Keyspace getKeyspace() {
        if (keyspace == null) {
            parse();
        }
        return keyspace;
    }

    private void parse() {
        try {
            // Avoid CodedInputStream creating a huge buffer
            byte[] all = ByteBuffers.toArray(asBytes());
            assert all.length < 4096; // This is only a win if the buffer is small

            CodedInputStream c = CodedInputStream.newInstance(all);
            int v = c.readInt32();

            int remaining = all.length - c.getTotalBytesRead();
            byte[] id = c.readRawBytes(remaining);

            this.keyspace = Keyspace.fromId(v);
            this.unqualified = ByteString.copyFrom(id);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
