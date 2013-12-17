package com.cloudata.btree;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;

public class Keyspace {
    public static final Keyspace ZERO = new Keyspace(0);

    final int keyspaceId;
    final ByteString keyspaceIdPrefix;

    public Keyspace(int keyspaceId) {
        Preconditions.checkArgument(keyspaceId >= 0);
        this.keyspaceId = keyspaceId;

        if (keyspaceId < 128) {
            assert 1 == CodedOutputStream.computeInt32SizeNoTag(keyspaceId);
            byte[] data = new byte[1];
            data[0] = (byte) keyspaceId;
            this.keyspaceIdPrefix = ByteString.copyFrom(data);
        } else {
            try {
                byte[] data = new byte[CodedOutputStream.computeInt32SizeNoTag(keyspaceId)];
                CodedOutputStream c = CodedOutputStream.newInstance(data);
                c.writeInt32NoTag(keyspaceId);
                c.flush();

                this.keyspaceIdPrefix = ByteString.copyFrom(data);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public int getKeyspaceId() {
        return keyspaceId;
    }

    public ByteString mapToKey(ByteString key) {
        return keyspaceIdPrefix.concat(key);
    }

    public static Keyspace build(int keyspaceId) {
        return new Keyspace(keyspaceId);
    }

    public ByteString mapToKey(byte[] key) {
        return mapToKey(ByteString.copyFrom(key));
    }
}
