package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import com.google.common.util.concurrent.ListenableFuture;

public class ScsiInquiryRequest extends ScsiCommandRequest {

    public static final byte SCSI_CODE = 0x12;

    private static final byte CODEPAGE_SUPPORTED_VPD_PAGES = 0x0;
    private static final byte CODEPAGE_DEVICE_IDENTIFICATION = (byte) 0x83;

    public ScsiInquiryRequest(IscsiSession session, ByteBuf buf) {
        super(session, buf);

        assert getByte(CDB_START) == SCSI_CODE;
    }

    @Override
    public ListenableFuture<Void> start() {
        ScsiDataInResponse response = new ScsiDataInResponse();
        populateResponseFields(session, response);

        boolean evpd = (getByte(CDB_START + 1) & 0x1) != 0;
        byte codepage = getByte(CDB_START + 2);
        int expectedLength = getShort(CDB_START + 3);

        if (evpd) {
            switch (codepage) {
            case CODEPAGE_SUPPORTED_VPD_PAGES:
                addSupportedVpdPages(response);
                break;

            default:
                throw new UnsupportedOperationException();
            }
        } else {
            addNormalInquiry(response);
        }

        int dataLength = 0;
        if (response.data != null) {
            dataLength = response.data.readableBytes();
        }

        response.setResiduals(expectedLength, dataLength);

        return sendFinal(response);
    }

    private void addNormalInquiry(ScsiDataInResponse response) {
        response.setStatus((byte) 0x0);

        int length = 36;
        ByteBuf data = Unpooled.buffer(length);

        // PERIPHERAL QUALIFIER
        // PERIPHERAL DEVICE TYPE
        data.writeByte(0);

        // RMB: Not removable
        data.writeByte(0);

        // Version: SPC-4
        data.writeByte(0x5);

        // Obsolete
        // Obsolete
        // NORMACA
        // HISUP
        // RESPONSE DATA FORMAT
        data.writeByte(2);

        // ADDITIONAL LENGTH
        data.writeByte(length - data.writerIndex() - 1);

        // SCCS
        // ACC
        // TPGS
        // 3PC
        // Reserved
        // PROTECT
        data.writeByte(0xb0);

        // Obsolete Formerly BQUE
        // ENCSERV
        // VS
        // MULTIP
        // MCHNGR
        // Obsolete
        // Obsolete
        // ADDR16 a
        data.writeByte(0x00);

        // Obsolete
        // Obsolete
        // WBUS16a
        // SYNC a
        // Obsolete Formerly LINKED
        // Obsolete
        // CMDQUE
        // VS
        data.writeByte(0x2);

        // T10 VENDOR IDENTIFICATION
        writeFixedString(data, "Cloudata", 8);

        // PRODUCT IDENTIFICATION
        writeFixedString(data, "iSCSI Target", 16);

        // PRODUCT REVISION LEVEL
        writeFixedString(data, "0.1", 4);

        assert data.writerIndex() == length;
        response.data = data;
    }

    private void writeFixedString(final ByteBuf dst, final String s, final int length) {
        int stringLength = s.length();
        if (length < stringLength) {
            throw new IllegalArgumentException();
        }

        for (int i = 0; i < stringLength; i++) {
            dst.writeByte(s.charAt(i));
        }

        for (int i = stringLength; i < length; i++) {
            dst.writeByte(0);
        }
    }

    private void addSupportedVpdPages(ScsiDataInResponse response) {
        ByteBuf data = Unpooled.buffer();
        data.writeByte(0);
        data.writeByte(0);

        // Number of pages...
        data.writeByte(2);

        data.writeByte(CODEPAGE_SUPPORTED_VPD_PAGES);
        data.writeByte(CODEPAGE_DEVICE_IDENTIFICATION);

        response.data = data;
    }

}
