package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.blockstore.Volume;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class ScsiWriteRequest extends ScsiCommandRequest {

    private static final Logger log = LoggerFactory.getLogger(ScsiWriteRequest.class);

    public static final byte SCSI_CODE_WRITE_16 = (byte) 0x8a;

    private static final byte FLAG_FUA = 0x08;

    final byte flags;

    final long lba;

    final int blockCount;

    final byte control;

    final byte groupNumber;

    final int expectedDataTransferLength;

    final SettableFuture<Void> future;

    final Volume volume;

    final boolean fua;

    public ScsiWriteRequest(IscsiSession session, ByteBuf buf) {
        super(session, buf);

        this.expectedDataTransferLength = getInt(20);

        this.flags = getByte(CDB_START + 1);
        switch (getByte(CDB_START)) {
        case SCSI_CODE_WRITE_16:
            this.lba = getLong(CDB_START + 2);
            this.blockCount = getInt(CDB_START + 10);
            this.groupNumber = getByte(CDB_START + 14);
            this.control = getByte(CDB_START + 15);
            break;

        default:
            throw new IllegalStateException();
        }

        this.fua = (flags & FLAG_FUA) != 0;

        this.volume = session.getVolume(lun);

        this.start = lba * session.getBlockSize();
        long length = blockCount * session.getBlockSize();
        this.pos = this.start;
        this.end = this.start + length;

        int chunkSize = volume.getChunkSize();
        if ((this.start % chunkSize) != 0) {
            this.nextChunkEnd = this.start - (this.start % chunkSize) + (chunkSize * 2);
        } else {
            this.nextChunkEnd = this.start + chunkSize;
        }

        this.buffer = Unpooled.compositeBuffer();
        this.bufferStart = start;
        this.future = SettableFuture.create();
    }

    final long start;
    final long end;
    long pos;

    long nextChunkEnd;

    long bufferStart;
    final CompositeByteBuf buffer;

    int nextR2TSN = 0;

    synchronized void addData(long diskOffset, ByteBuf data) {
        if (diskOffset != pos) {
            log.warn("Data out of order");
            throw new IllegalArgumentException();
        }

        // log.debug("Adding buffer: {}", ByteBufUtil.hexDump(data));
        data.retain();

        buffer.addComponent(data);
        buffer.writerIndex(buffer.writerIndex() + data.readableBytes());
        pos += data.readableBytes();

        assert pos == (bufferStart + buffer.readableBytes());

        while (pos >= this.nextChunkEnd) {
            int n = Ints.checkedCast(nextChunkEnd - bufferStart);
            assert buffer.readableBytes() >= n;

            ByteBuf slice = buffer.slice(buffer.readerIndex(), n);

            volume.write(bufferStart, n, slice);

            buffer.skipBytes(n);
            bufferStart += n;
            // buffer.discardSomeReadBytes();

            nextChunkEnd += volume.getChunkSize();
        }

        if (pos >= end) {
            Preconditions.checkState(pos == end);

            if (buffer.isReadable()) {
                int n = buffer.readableBytes();
                volume.write(bufferStart, n, buffer);

                buffer.skipBytes(n);
                bufferStart += n;
            }

            // buffer.discardSomeReadBytes();

            Preconditions.checkState(bufferStart == end);

            ListenableFuture<Void> syncFuture;

            if (fua) {
                // TODO: Do we have to do a full sync? Can we just sync the written range?
                // (Is this allowed? Is it a win?)

                syncFuture = volume.sync();
            } else {
                syncFuture = Futures.immediateFuture(null);
            }

            Futures.addCallback(syncFuture, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result) {
                    ScsiResponse response = new ScsiResponse();
                    populateResponseFields(session, response);

                    response.setStatus(ResponseCode.CompletedAtTarget, ScsiStatus.Good);

                    ChannelFuture sendFuture = session.send(response, true);
                    chain(future, sendFuture);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Error during write", t);
                    future.setException(t);
                }
            });

        }
    }

    synchronized void startNextTransfer() {
        if (this.pos < this.end) {
            R2TResponse r2t = new R2TResponse();
            populateResponseFields(session, r2t);

            r2t.lun = ScsiWriteRequest.this.lun;

            int length = 4 * 1024 * 1024;

            // Try to align to chunks
            length += this.nextChunkEnd - this.start;

            if ((end - pos) < length) {
                length = Ints.checkedCast(end - pos);
            }

            int transferOffset = Ints.checkedCast(this.pos - this.start);
            Transfer transfer = session.createTransfer(transferOffset, length, new TransferListener() {

                @Override
                public void gotData(int bufferOffset, ByteBuf data) {
                    long diskOffset = bufferOffset + start;
                    addData(diskOffset, data);
                }

                @Override
                public void endOfData() {
                    // TODO: Avoid round-trip?
                    startNextTransfer();
                }
            });

            r2t.targetTransferTag = transfer.targetTransferTag;

            r2t.r2tsn = nextR2TSN++;
            r2t.bufferOffset = transferOffset;
            r2t.desiredDataTransferLength = length;

            session.send(r2t, true);
        }
    }

    @Override
    public ListenableFuture<Void> start() {
        Preconditions.checkState(!future.isDone());

        ByteBuf data = getData();
        if (data != null) {
            addData(this.start + 0, data);
        }

        startNextTransfer();

        return future;
    }

    @Override
    protected void deallocate() {
        super.deallocate();

        this.buffer.release();
    }

    @Override
    public String toString() {
        return "ScsiWriteRequest [flags=" + flags + ", lba=" + lba + ", blockCount=" + blockCount + "]";
    }

}
