package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

import java.nio.ByteOrder;

import com.cloudata.blockstore.Volume;

public abstract class ScsiCommandRequest extends IscsiRequest {

    public static final int OPCODE = 0x01;

    public static final int CDB_START = 32;

    public final byte flags;

    public final long lun;

    public final Volume volume;

    public ScsiCommandRequest(IscsiSession session, ByteBuf buf) {
        super(session, buf);
        assert buf.order() == ByteOrder.BIG_ENDIAN;
        assert getOpcode() == OPCODE;

        this.flags = getByte(1);

        this.lun = getLong(8);

        this.volume = session.getVolume(lun);
    }

}
