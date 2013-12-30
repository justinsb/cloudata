package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.Closeable;
import java.nio.ByteOrder;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

public abstract class IscsiRequest implements Closeable {
    public final IscsiSession session;

    protected final ByteBuf buf;
    public final int initiatorTaskTag;
    public final int totalAhsLength;
    public final int totalDataLength;
    public final boolean immediate;

    public IscsiRequest(IscsiSession session, ByteBuf buf) {
        this.session = session;
        this.buf = buf;
        this.buf.retain();

        assert buf.order() == ByteOrder.BIG_ENDIAN;

        this.immediate = BasicHeaderSegment.isImmediate(buf);
        this.initiatorTaskTag = BasicHeaderSegment.getInitiatorTaskTag(buf);
        this.totalAhsLength = BasicHeaderSegment.getTotalAhsLength(buf);
        this.totalDataLength = BasicHeaderSegment.getDataSegmentLength(buf);
    }

    public int getDataLength() {
        return totalDataLength;
    }

    public ByteBuf getData() {
        if (totalDataLength == 0) {
            return null;
        }

        ByteBuf data = buf.duplicate();
        int readerIndex = data.readerIndex();
        readerIndex += BasicHeaderSegment.SIZE + totalAhsLength;
        data.readerIndex(readerIndex);
        data.writerIndex(readerIndex + totalDataLength);

        return data.slice();
    }

    public int getOpcode() {
        return BasicHeaderSegment.getOpcode(buf);
    }

    public ByteString getIsid() {
        return BasicHeaderSegment.getIsid(buf);
    }

    public int getCmdSN() {
        return BasicHeaderSegment.getCmdSN(buf);
    }

    public abstract ListenableFuture<Void> start();

    @Override
    public void close() {
        buf.release();
    }

    protected int getShort(int offset) {
        return buf.getShort(buf.readerIndex() + offset);

        // int readerIndex = buf.readerIndex();
        //
        // int v = (buf.getByte(readerIndex + offset) & 0xff) << 8;
        // v |= (buf.getByte(readerIndex + offset + 1) & 0xff);
        // return v;
    }

    protected int getInt(int offset) {
        return buf.getInt(buf.readerIndex() + offset);

        // int readerIndex = buf.readerIndex();
        //
        // int v = (buf.getByte(readerIndex + offset) & 0xff) << 24;
        // v |= (buf.getByte(readerIndex + offset + 1) & 0xff) << 16;
        // v |= (buf.getByte(readerIndex + offset + 2) & 0xff) << 8;
        // v |= (buf.getByte(readerIndex + offset + 3) & 0xff);
        //
        // assert v == buf.getInt(readerIndex + offset);
        //
        // return v;
    }

    protected long getLong(int offset) {
        return buf.getLong(offset);
    }

    protected byte getByte(int offset) {
        return buf.getByte(buf.readerIndex() + offset);
    }

    public int getInitiatorTaskTag() {
        return initiatorTaskTag;
    }

    protected void populateResponseFields(IscsiSession session, IscsiResponse response) {
        response.initiatorTaskTag = getInitiatorTaskTag();

        response.statSN = session.getNextStatSN();

        int cmdSN = getCmdSN();
        response.expectedCommandSN = cmdSN + 1;
        response.maxCommandSN = cmdSN + 32;
    }

    protected ListenableFuture<Void> sendFinal(IscsiResponse response) {
        ChannelFuture future = session.send(response, true);

        SettableFuture<Void> settableFuture = SettableFuture.create();
        chain(settableFuture, future);
        return settableFuture;
    }

    protected void chain(final SettableFuture<Void> settableFuture, ChannelFuture future) {
        future.addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                Throwable error = future.cause();
                if (error != null) {
                    settableFuture.setException(error);
                } else {
                    assert future.isSuccess();
                    settableFuture.set(null);
                }

            }
        });
    }
}
