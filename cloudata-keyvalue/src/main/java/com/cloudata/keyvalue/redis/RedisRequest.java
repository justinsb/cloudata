package com.cloudata.keyvalue.redis;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.btree.ByteBuffers;
import com.google.protobuf.ByteString;

public class RedisRequest {
    final byte[][] command;
    final boolean inline;

    public RedisRequest(byte[] inlineCommand, boolean inline) {
        if (!inline) {
            throw new IllegalArgumentException();
        }
        this.command = new byte[][] { inlineCommand };
        this.inline = inline;
    }

    public RedisRequest(byte[][] command) {
        this.command = command;
        this.inline = false;
    }

    public boolean isInline() {
        return inline;
    }

    public byte[] getName() {
        return command[0];
    }

    public byte[] get(int i) {
        return command[i];
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [");
        for (int i = 0; i < command.length; i++) {
            if (i != 0) {
                sb.append(",");
            }
            byte[] a = command[i];
            if (a == null) {
                sb.append("(null)");
            }
            sb.append(new String(a));
        }
        if (inline) {
            sb.append(" (inline)");
        }
        sb.append("]");
        return sb.toString();
    }

    public int getArgc() {
        return command.length;
    }

    public long getLong(int i) {
        byte[] data = get(i);

        long v = ByteBuffers.parseLong(ByteBuffer.wrap(data));

        return v;
    }

    public ByteString getByteString(int i) {
        byte[] data = get(i);
        return ByteString.copyFrom(data);
    }

}
