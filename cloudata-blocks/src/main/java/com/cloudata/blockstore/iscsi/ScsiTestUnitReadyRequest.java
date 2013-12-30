package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

import com.google.common.util.concurrent.ListenableFuture;

public class ScsiTestUnitReadyRequest extends ScsiCommandRequest {

    public static final int SCSI_CODE = 0x0;

    public ScsiTestUnitReadyRequest(IscsiSession session, ByteBuf buf) {
        super(session, buf);

        assert getByte(CDB_START) == SCSI_CODE;
    }

    @Override
    public ListenableFuture<Void> start() {
        ScsiResponse response = new ScsiResponse();
        populateResponseFields(session, response);

        return sendFinal(response);
    }

}
