package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Transfer {

    private static final Logger log = LoggerFactory.getLogger(Transfer.class);

    final int targetTransferTag;
    final TransferListener listener;

    final int bufferStart;
    final int bufferEnd;
    int bufferPos;

    private final IscsiSession session;

    public Transfer(IscsiSession session, int targetTransferTag, int bufferStart, int length, TransferListener listener) {
        this.session = session;
        this.targetTransferTag = targetTransferTag;
        this.listener = listener;
        this.bufferStart = bufferStart;
        this.bufferEnd = this.bufferStart + length;
        this.bufferPos = bufferStart;
    }

    public void gotData(ScsiDataOutRequest scsiDataOutRequest) {
        int bufferOffset = scsiDataOutRequest.bufferOffset;
        ByteBuf data = scsiDataOutRequest.getData();

        if (bufferOffset != bufferPos) {
            log.warn("Got transfer data out of order: {} vs {}", bufferOffset, bufferPos);
            throw new IllegalArgumentException();
        }

        int length = data.readableBytes();

        if ((bufferPos + length) > bufferEnd) {
            log.warn("Got transfer after end");
            throw new IllegalArgumentException();
        }

        listener.gotData(bufferOffset, data);

        bufferPos += length;

        if (bufferPos >= bufferEnd) {
            listener.endOfData();
            session.endOfTransfer(this);
        }

    }

}