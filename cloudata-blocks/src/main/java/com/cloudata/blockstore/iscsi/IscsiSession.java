package com.cloudata.blockstore.iscsi;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.util.Map;
import java.util.Random;

import com.cloudata.blockstore.Volume;
import com.cloudata.btree.Keyspace;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class IscsiSession {

    private static final AttributeKey<IscsiSession> KEY = AttributeKey.valueOf(IscsiSession.class.getSimpleName());

    private final IscsiServer server;

    Keyspace keyspace = Keyspace.ZERO;

    private int nextStatSN;
    private int nextTargetTransferTag;

    public Keyspace getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(Keyspace keyspace) {
        this.keyspace = keyspace;
    }

    static final Random random = new Random();

    private final ChannelHandlerContext ctx;

    final short targetSessionIdentifier;

    private final Map<Integer, Transfer> transfers = Maps.newHashMap();

    public IscsiSession(IscsiServer server, short targetSessionIdentifier, ChannelHandlerContext ctx) {
        this.server = server;
        this.targetSessionIdentifier = targetSessionIdentifier;
        this.ctx = ctx;

        synchronized (random) {
            this.nextStatSN = random.nextInt();
        }
    }

    public static IscsiSession get(IscsiServer server, ChannelHandlerContext ctx) {
        Attribute<IscsiSession> attribute = ctx.attr(KEY);
        while (true) {
            IscsiSession redisSession = attribute.get();
            if (redisSession != null) {
                return redisSession;
            }

            redisSession = new IscsiSession(server, server.assignSessionIdentifier(), ctx);
            if (attribute.compareAndSet(null, redisSession)) {
                return redisSession;
            }
        }
    }

    public int getBlockSize() {
        return 512;
    }

    public int getNextStatSN() {
        return nextStatSN++;
    }

    public ChannelFuture send(IscsiResponse message, boolean flush) {
        Preconditions.checkNotNull(message);

        ChannelFuture future = ctx.write(message);

        if (flush) {
            ctx.flush();
        }

        return future;
    }

    public short getTargetSessionIdentifier() {
        return targetSessionIdentifier;
    }

    public Volume getVolume(long lun) {
        return server.getVolume(lun);
    }

    public Transfer createTransfer(int bufferStart, int length, TransferListener transferListener) {
        int targetTransferTag;
        synchronized (this) {
            targetTransferTag = nextTargetTransferTag++;
        }
        if (targetTransferTag == 0xffffffff) {
            throw new IllegalStateException();
        }

        Transfer transfer = new Transfer(this, targetTransferTag, bufferStart, length, transferListener);
        synchronized (transfers) {
            if (transfers.containsKey(targetTransferTag)) {
                throw new IllegalStateException();
            }

            transfers.put(targetTransferTag, transfer);
        }

        return transfer;
    }

    public Transfer getTransfer(int targetTransferTag) {
        synchronized (transfers) {
            return transfers.get(targetTransferTag);
        }
    }

    void endOfTransfer(Transfer transfer) {
        synchronized (transfers) {
            transfers.remove(transfer.targetTransferTag);
        }
    }

}
