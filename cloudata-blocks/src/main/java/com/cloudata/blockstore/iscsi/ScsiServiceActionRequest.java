package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import com.google.common.util.concurrent.ListenableFuture;

public class ScsiServiceActionRequest extends ScsiCommandRequest {

    public static final byte SCSI_CODE = (byte) 0x9e;

    private static final byte READ_CAPACITY_16 = 0x10;

    public ScsiServiceActionRequest(IscsiSession session, ByteBuf buf) {
        super(session, buf);

        assert getByte(CDB_START) == SCSI_CODE;
    }

    @Override
    public ListenableFuture<Void> start() {
        ScsiDataInResponse response = new ScsiDataInResponse();
        populateResponseFields(session, response);

        byte action = getByte(CDB_START + 1);
        int allocationLength = getInt(CDB_START + 10);

        switch (action) {
        case READ_CAPACITY_16:
            sendCapacity(session, response);
            break;

        default:
            throw new UnsupportedOperationException();
        }

        int dataLength = 0;
        if (response.data != null) {
            dataLength = response.data.readableBytes();
        }

        response.setResiduals(allocationLength, dataLength);

        return sendFinal(response);
    }

    private void sendCapacity(IscsiSession session, ScsiDataInResponse response) {
        response.setStatus((byte) 0);

        int length = 32;
        ByteBuf data = Unpooled.buffer(length);

        long blocks = session.getBlockCount();
        int blockSize = session.getBlockSize();

        data.writeLong(blocks);
        data.writeInt(blockSize);

        // Reserved
        // P_TYPE PROT_EN
        data.writeByte(0);

        // P_I_EXPONENT
        // LOGICAL BLOCKS PER PHYSICAL BLOCK EXPONENT
        data.writeByte(0);

        // TPE
        // TPRZ
        // LOWEST ALIGNED LOGICAL BLOCK ADDRESS
        data.writeByte(0);
        data.writeByte(0);

        // Reserved (16)
        data.writeZero(16);

        assert data.writerIndex() == length;
        response.data = data;
    }

}
