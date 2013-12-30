package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

import com.google.protobuf.ByteString;

public class BasicHeaderSegment {
    public static final int SIZE = 48;

    public static int getTotalAhsLength(ByteBuf buf) {
        byte totalAhsLength = buf.getByte(buf.readerIndex() + 4);
        int length = 4 * (totalAhsLength & 0xff);
        return length;
    }

    public static int getDataSegmentLength(ByteBuf buf) {
        int length = (buf.getByte(buf.readerIndex() + 5) & 0xff) << 16
                | (buf.getByte(buf.readerIndex() + 6) & 0xff) << 8 | (buf.getByte(buf.readerIndex() + 7) & 0xff);
        return length;
    }

    public static int getOpcode(ByteBuf buf) {
        int b = buf.getByte(buf.readerIndex() + 0) & 0xff;
        return b & 0x3f;
    }

    public static boolean isImmediate(ByteBuf buf) {
        return 0 != (buf.getByte(buf.readerIndex() + 0) & 0x40);
    }

    public static ByteString getIsid(ByteBuf buf) {
        byte[] v = new byte[6];
        buf.getBytes(buf.readerIndex() + 8, v);

        return ByteString.copyFrom(v);
    }

    public static int getInitiatorTaskTag(ByteBuf buf) {
        int tag = buf.getInt(buf.readerIndex() + 16);
        return tag;
    }

    public static int getCmdSN(ByteBuf buf) {
        int sn = buf.getInt(buf.readerIndex() + 24);
        return sn;
    }

}
