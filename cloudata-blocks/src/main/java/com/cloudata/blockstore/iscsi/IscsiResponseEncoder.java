package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IscsiResponseEncoder extends MessageToByteEncoder<IscsiResponse> {
    private static final Logger log = LoggerFactory.getLogger(IscsiResponseEncoder.class);

    @Override
    public void encode(ChannelHandlerContext ctx, IscsiResponse msg, ByteBuf out) throws Exception {
        try {
            msg.encode(out);
        } catch (Throwable t) {
            log.warn("Error during encoding", t);
            throw t;
        }
    }
}