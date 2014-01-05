package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

public class NopResponse extends IscsiResponse {
    public long lun = 0;
    public int targetTransferTag = 0xffffffff;

    private ByteBuf data;

    @Override
    public void encode(ByteBuf buf) {
        int startWriterIndex = buf.writerIndex();

        int dataSegmentLength = data != null ? data.readableBytes() : 0;

        buf.writeByte(0x20);

        {
            byte flags = 0x0;
            if (flagFinal) {
                flags |= 0x80;
            }
            buf.writeByte(flags);
        }

        buf.writeByte(0);
        buf.writeByte(0);

        int ahsLength = 0 >> 2;
        buf.writeByte(ahsLength);

        buf.writeByte(dataSegmentLength >> 16);
        buf.writeByte(dataSegmentLength >> 8);
        buf.writeByte(dataSegmentLength >> 0);

        buf.writeLong(lun);

        buf.writeInt(initiatorTaskTag);
        buf.writeInt(targetTransferTag);

        buf.writeInt(statSN);

        buf.writeInt(expectedCommandSN);
        buf.writeInt(maxCommandSN);

        buf.writeZero(12);

        assert buf.writerIndex() == startWriterIndex + 48;

        if (data != null) {
            throw new UnsupportedOperationException();

            // buf.writeBytes(data);
            //
            // {
            // // Pad the data segment
            // int pad = 4 - (dataSegmentLength % 4);
            // if (pad != 4) {
            // buf.writeZero(pad);
            // }
            // }
        }

    }

    @Override
    protected void deallocate() {
        if (data != null) {
            data.release();
        }
    }
}
