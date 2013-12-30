package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IscsiRequestDecoder extends ByteToMessageDecoder {

    private static final Logger log = LoggerFactory.getLogger(IscsiRequestDecoder.class);

    private final IscsiServer server;

    public IscsiRequestDecoder(IscsiServer server) {
        this.server = server;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        while (true) {
            if (!in.isReadable(BasicHeaderSegment.SIZE)) {
                return;
            }

            // TODO: Header digest / data digest
            int totalLength = BasicHeaderSegment.SIZE + BasicHeaderSegment.getTotalAhsLength(in)
                    + BasicHeaderSegment.getDataSegmentLength(in);
            int padding = 4 - (totalLength % 4);
            if (padding == 4) {
                padding = 0;
            }

            if (!in.isReadable(totalLength + padding)) {
                return;
            }

            int opcode = BasicHeaderSegment.getOpcode(in);
            ByteBuf slice = in.readSlice(totalLength);
            slice = slice.order(ByteOrder.BIG_ENDIAN);

            if (padding != 0) {
                in.skipBytes(padding);
            }

            IscsiSession session = IscsiSession.get(server, ctx);

            switch (opcode) {
            case LoginRequest.OPCODE: {
                out.add(new LoginRequest(session, slice));
                break;
            }

            case NopRequest.OPCODE: {
                out.add(new NopRequest(session, slice));
                break;
            }

            case ScsiDataOutRequest.OPCODE: {
                out.add(new ScsiDataOutRequest(session, slice));
                break;
            }

            case ScsiCommandRequest.OPCODE: {
                byte scsiOpcode = slice.getByte(slice.readerIndex() + 32);
                switch (scsiOpcode) {
                case ScsiTestUnitReadyRequest.SCSI_CODE:
                    out.add(new ScsiTestUnitReadyRequest(session, slice));
                    break;

                case ScsiInquiryRequest.SCSI_CODE:
                    out.add(new ScsiInquiryRequest(session, slice));
                    break;

                case ScsiServiceActionRequest.SCSI_CODE:
                    out.add(new ScsiServiceActionRequest(session, slice));
                    break;

                case ScsiReadRequest.SCSI_CODE_READ_16:
                    out.add(new ScsiReadRequest(session, slice));
                    break;

                case ScsiWriteRequest.SCSI_CODE_WRITE_16:
                    out.add(new ScsiWriteRequest(session, slice));
                    break;

                case ScsiSynchronizeCacheRequest.SCSI_CODE_SYNCHRONIZE_CACHE_10:
                case ScsiSynchronizeCacheRequest.SCSI_CODE_SYNCHRONIZE_CACHE_16:
                    out.add(new ScsiSynchronizeCacheRequest(session, slice));
                    break;

                default:
                    log.warn("Unsupported scsi opcode: {}", scsiOpcode);
                    throw new IOException();
                }
                break;
            }

            default:
                log.warn("Opcode not supported: {}", opcode);
                throw new IOException("Opcode not supported");
            }
        }
    }

}
