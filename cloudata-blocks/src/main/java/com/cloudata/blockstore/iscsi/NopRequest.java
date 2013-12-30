package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

import com.google.common.util.concurrent.ListenableFuture;

public class NopRequest extends IscsiRequest {

    public static final int OPCODE = 0x00;

    final int targetTransferTag;

    final long lun;

    public NopRequest(IscsiSession session, ByteBuf buf) {
        super(session, buf);
        assert getOpcode() == OPCODE;

        this.targetTransferTag = getInt(20);
        this.lun = getLong(8);
    }

    @Override
    public ListenableFuture<Void> start() {
        NopResponse response = new NopResponse();
        populateResponseFields(session, response);

        response.targetTransferTag = this.targetTransferTag;
        response.lun = this.lun;

        return sendFinal(response);
    }

}
