package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import com.google.common.util.concurrent.ListenableFuture;

public class ScsiReadRequest extends ScsiCommandRequest {

    public static final byte SCSI_CODE_READ_16 = (byte) 0x88;

    final byte flags;

    final long lba;

    final int blockCount;

    final byte control;

    final byte groupNumber;

    final int expectedDataTransferLength;

    public ScsiReadRequest(IscsiSession session, ByteBuf buf) {
        super(session, buf);

        this.expectedDataTransferLength = getInt(20);

        this.flags = getByte(CDB_START + 1);
        switch (getByte(CDB_START)) {
        case SCSI_CODE_READ_16:
            this.lba = getLong(CDB_START + 2);
            this.blockCount = getInt(CDB_START + 10);
            this.groupNumber = getByte(CDB_START + 14);
            this.control = getByte(CDB_START + 15);
            break;

        default:
            throw new IllegalStateException();
        }
    }

    @Override
    public ListenableFuture<Void> start() {
        ScsiDataInResponse response = new ScsiDataInResponse();
        populateResponseFields(session, response);

        sendReadResponse(session, response);

        int dataLength = 0;
        if (response.data != null) {
            dataLength = response.data.readableBytes();
        }

        response.setResiduals(expectedDataTransferLength, dataLength);

        return sendFinal(response);
    }

    private void sendReadResponse(IscsiSession session, ScsiDataInResponse response) {
        response.setStatus((byte) 0);

        int dataLength = this.blockCount * session.getBlockSize();
        ByteBuf data = Unpooled.buffer(dataLength);

        data.writeZero(dataLength);

        assert data.writerIndex() == dataLength;
        response.data = data;
    }

}
