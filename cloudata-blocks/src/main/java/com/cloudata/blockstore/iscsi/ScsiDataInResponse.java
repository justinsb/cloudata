package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

import com.google.common.base.Preconditions;

public class ScsiDataInResponse extends IscsiResponse {

    public boolean flagAcknowledge;
    public boolean flagHasStatus;

    public long lun;
    public int targetTransferTag = 0xffffffff;

    public int dataSN;
    protected byte status;

    public int bufferOffset;
    public int residualCount;

    private ByteBuf data;

    @Override
    public void encode(ByteBuf buf) {
        int startWriterIndex = buf.writerIndex();

        int dataSegmentLength = data != null ? data.readableBytes() : 0;

        buf.writeByte(0x25);

        {
            int b = 0x0;
            if (flagFinal) {
                b |= 0x80;
            }
            if (flagAcknowledge) {
                b |= 0x40;
            }
            if (flagResidualOverflow) {
                b |= 0x4;
            }
            if (flagResidualUnderflow) {
                b |= 0x2;
            }
            if (flagHasStatus) {
                b |= 0x1;
            }
            buf.writeByte(b);
        }

        buf.writeByte(0);
        buf.writeByte(status);

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

        buf.writeInt(dataSN);

        buf.writeInt(bufferOffset);
        buf.writeInt(residualCount);

        assert buf.writerIndex() == startWriterIndex + 48;

        if (data != null) {
            buf.writeBytes(data);

            {
                // Pad the data segment
                int pad = 4 - (dataSegmentLength % 4);
                if (pad != 4) {
                    buf.writeZero(pad);
                }
            }
        }

    }

    public void setStatus(byte status) {
        this.flagHasStatus = true;
        this.status = status;
    }

    @Override
    protected void deallocate() {
        if (data != null) {
            data.release();
            data = null;
        }
    }

    public void setData(ByteBuf data, boolean addRef) {
        Preconditions.checkState(this.data == null);

        this.data = data.duplicate();

        if (addRef) {
            this.data.retain();
        }
    }

    public int getDataLength() {
        if (data != null) {
            return data.readableBytes();
        }
        return 0;
    }

}
