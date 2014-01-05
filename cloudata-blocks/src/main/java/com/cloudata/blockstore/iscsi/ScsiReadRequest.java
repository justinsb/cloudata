package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class ScsiReadRequest extends ScsiCommandRequest {
    private static final Logger log = LoggerFactory.getLogger(ScsiReadRequest.class);

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
        final SettableFuture<Void> opFuture = SettableFuture.create();

        int blockSize = session.getBlockSize();

        final long length = this.blockCount * blockSize;
        long offset = this.lba * blockSize;

        ListenableFuture<ByteBuf> readFuture = volume.read(offset, length);

        Futures.addCallback(readFuture, new FutureCallback<ByteBuf>() {

            @Override
            public void onSuccess(ByteBuf data) {
                ScsiDataInResponse response = new ScsiDataInResponse();
                populateResponseFields(session, response);

                response.setStatus((byte) 0);

                response.setData(data, false);

                int dataLength = 0;
                if (data != null) {
                    dataLength = data.readableBytes();
                    assert dataLength == length;
                }

                response.setResiduals(expectedDataTransferLength, dataLength);

                ChannelFuture future = session.send(response, true);

                chain(opFuture, future);
            }

            @Override
            public void onFailure(Throwable t) {
                log.warn("Error during read", t);
                opFuture.setException(t);
            }
        });

        return opFuture;
    }

    @Override
    public String toString() {
        return "ScsiReadRequest [lba=" + lba + ", blockCount=" + blockCount + "]";
    }

}
