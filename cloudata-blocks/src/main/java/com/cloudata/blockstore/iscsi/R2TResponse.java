package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

public class R2TResponse extends IscsiResponse {

    public long lun;
    public int targetTransferTag = 0xffffffff;

    public int r2tsn;
    public int bufferOffset;
    public int desiredDataTransferLength;

    @Override
    public void encode(ByteBuf buf) {
        // Preconditions.checkState(data == null);

        int startWriterIndex = buf.writerIndex();

        buf.ensureWritable(BasicHeaderSegment.SIZE);

        buf.writeByte(0x31);
        {
            byte flags = 0x0;
            if (flagFinal) {
                flags |= 0x80;
            }
            buf.writeByte(flags);
        }
        // reserved
        buf.writeByte(0);
        buf.writeByte(0);

        // AHS length
        buf.writeByte(0);
        // Data Segment length
        buf.writeZero(3);

        buf.writeLong(lun);

        buf.writeInt(initiatorTaskTag);
        buf.writeInt(targetTransferTag);

        buf.writeInt(statSN);

        buf.writeInt(expectedCommandSN);
        buf.writeInt(maxCommandSN);

        buf.writeInt(r2tsn);
        buf.writeInt(bufferOffset);
        buf.writeInt(desiredDataTransferLength);

        assert buf.writerIndex() == startWriterIndex + 48;
    }
}
