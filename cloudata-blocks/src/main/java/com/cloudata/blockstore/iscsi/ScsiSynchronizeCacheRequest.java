package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class ScsiSynchronizeCacheRequest extends ScsiCommandRequest {

    public static final byte SCSI_CODE_SYNCHRONIZE_CACHE_10 = (byte) 0x35;
    public static final byte SCSI_CODE_SYNCHRONIZE_CACHE_16 = (byte) 0x91;

    final byte flags;

    final long lba;

    final int blockCount;

    final byte control;

    final byte groupNumber;

    final int expectedDataTransferLength;

    public ScsiSynchronizeCacheRequest(IscsiSession session, ByteBuf buf) {
        super(session, buf);

        this.expectedDataTransferLength = getInt(20);

        this.flags = getByte(CDB_START + 1);
        switch (getByte(CDB_START)) {
        case SCSI_CODE_SYNCHRONIZE_CACHE_10:
            this.lba = getInt(CDB_START + 2);
            this.groupNumber = getByte(CDB_START + 6);
            this.blockCount = getShort(CDB_START + 7);
            this.control = getByte(CDB_START + 9);
            break;

        case SCSI_CODE_SYNCHRONIZE_CACHE_16:
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
        ListenableFuture<Void> syncFuture = volume.sync();
        return Futures.transform(syncFuture, new AsyncFunction<Void, Void>() {
            @Override
            public ListenableFuture<Void> apply(Void o) {
                ScsiResponse response = new ScsiResponse();
                populateResponseFields(session, response);
                return sendFinal(response);
            }
        });
    }

}
