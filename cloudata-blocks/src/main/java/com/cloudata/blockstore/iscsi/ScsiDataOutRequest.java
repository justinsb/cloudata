package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class ScsiDataOutRequest extends IscsiRequest {

    public static final int OPCODE = 0x05;

    private final long lun;

    private final int targetTransferTag;

    private final int dataSN;

    private final int expStatSN;

    public final int bufferOffset;

    private final byte flags;

    public ScsiDataOutRequest(IscsiSession session, ByteBuf buf) {
        super(session, buf);

        this.flags = getByte(1);
        this.lun = getLong(8);
        this.targetTransferTag = getInt(20);

        this.expStatSN = getInt(28);
        this.dataSN = getInt(36);
        this.bufferOffset = getInt(40);
        assert getOpcode() == OPCODE;
    }

    @Override
    public ListenableFuture<Void> start() {
        Transfer transfer = session.getTransfer(targetTransferTag);
        if (transfer == null) {
            throw new IllegalArgumentException();
        }

        transfer.gotData(this);

        return Futures.immediateFuture(null);
    }

}
