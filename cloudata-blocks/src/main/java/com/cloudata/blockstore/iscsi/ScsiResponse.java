package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

public class ScsiResponse extends IscsiResponse {
    public int snackTag;

    public int expectedDataSN;
    public int bidirectionalReadResidualCount;

    private ResponseCode responseCode = ResponseCode.CompletedAtTarget;
    private ScsiStatus status = ScsiStatus.Good;

    public ByteBuf data;

    @Override
    public void encode(ByteBuf buf) {
        int startWriterIndex = buf.writerIndex();

        int dataSegmentLength = data != null ? data.readableBytes() : 0;

        buf.writeByte(0x21);

        {
            int b = 0x0;
            if (flagFinal) {
                b |= 0x80;
            }
            if (flagResidualOverflow) {
                b |= 0x4;
            }
            if (flagResidualUnderflow) {
                b |= 0x2;
            }

            buf.writeByte(b);
        }

        buf.writeByte(responseCode.code);
        buf.writeByte(status.code);

        int ahsLength = 0 >> 2;
        buf.writeByte(ahsLength);

        buf.writeByte(dataSegmentLength >> 16);
        buf.writeByte(dataSegmentLength >> 8);
        buf.writeByte(dataSegmentLength >> 0);

        buf.writeZero(8);

        buf.writeInt(initiatorTaskTag);
        buf.writeInt(snackTag);

        buf.writeInt(statSN);

        buf.writeInt(expectedCommandSN);
        buf.writeInt(maxCommandSN);

        buf.writeInt(expectedDataSN);

        buf.writeInt(bidirectionalReadResidualCount);
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

    public void setStatus(ResponseCode responseCode, ScsiStatus status) {
        // this.flagHasStatus = true;
        this.responseCode = responseCode;
        this.status = status;
    }

}
