package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

import com.google.protobuf.ByteString;

public class LoginResponse extends IscsiResponse {
    public byte versionMax = 0;
    public byte versionActive = 0;

    public Stage currentStage;
    public Stage nextStage;

    public ByteString isid;

    public short targetSessionIdentifier;

    public ParameterData data = new ParameterData();

    public byte statusClass;
    public byte statusDetail;

    @Override
    public void encode(ByteBuf buf) {
        int startWriterIndex = buf.writerIndex();

        // Defer writing the header until we know how big the data is
        buf.ensureWritable(BasicHeaderSegment.SIZE + data.estimateSize());
        buf.writerIndex(startWriterIndex + BasicHeaderSegment.SIZE);

        data.write(buf);

        int dataSegmentLength = buf.writerIndex() - startWriterIndex - 48;

        {
            // Pad the data segment
            int pad = 4 - (dataSegmentLength % 4);
            if (pad != 4) {
                buf.writeZero(pad);
            }
        }

        int endWriterIndex = buf.writerIndex();

        // Write the header
        buf.writerIndex(startWriterIndex);

        buf.writeByte(0x23);

        {
            int b = 0x80;
            b |= currentStage.code << 2;
            b |= nextStage.code;

            buf.writeByte(b);
        }

        buf.writeByte(versionMax);
        buf.writeByte(versionActive);

        int ahsLength = 0 >> 2;
        buf.writeByte(ahsLength);

        buf.writeByte(dataSegmentLength >> 16);
        buf.writeByte(dataSegmentLength >> 8);
        buf.writeByte(dataSegmentLength >> 0);

        assert isid.size() == 6;
        buf.writeBytes(isid.asReadOnlyByteBuffer());

        assert targetSessionIdentifier != 0;
        buf.writeShort(targetSessionIdentifier);

        buf.writeInt(initiatorTaskTag);

        // reserved
        buf.writeInt(0);

        buf.writeInt(statSN);

        buf.writeInt(expectedCommandSN);
        buf.writeInt(maxCommandSN);

        buf.writeByte(statusClass);
        buf.writeByte(statusDetail);

        // reserved
        buf.writeZero(10);

        // Move to end
        buf.writerIndex(endWriterIndex);
    }

    @Override
    protected void deallocate() {

    }

}
