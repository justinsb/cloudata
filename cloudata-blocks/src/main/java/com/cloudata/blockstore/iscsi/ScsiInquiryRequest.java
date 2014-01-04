package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import com.google.common.util.concurrent.ListenableFuture;

public class ScsiInquiryRequest extends ScsiCommandRequest {

    public static final byte SCSI_CODE = 0x12;

    private static final byte CODEPAGE_SUPPORTED_VPD_PAGES = 0x0;
    private static final byte CODEPAGE_DEVICE_IDENTIFICATION = (byte) 0x83;

    final boolean evpd;

    final byte codepage;

    final int expectedLength;

    public ScsiInquiryRequest(IscsiSession session, ByteBuf buf) {
        super(session, buf);

        assert getByte(CDB_START) == SCSI_CODE;

        byte flags = getByte(CDB_START + 1);
        this.evpd = (flags & 0x1) != 0;
        this.codepage = getByte(CDB_START + 2);
        this.expectedLength = getShort(CDB_START + 3);
    }

    @Override
    public ListenableFuture<Void> start() {
        ScsiDataInResponse response = new ScsiDataInResponse();
        populateResponseFields(session, response);

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
        // 0 => Direct access block device, connected
        data.writeByte(0);

        // RMB
        // RESERVED
        // 0 => Not removable
        data.writeByte(0);

        // Version
        // 0x05 => SPC-4
        data.writeByte(0x5);

        // Obsolete
        // Obsolete
        // NORMACA
        // HISUP
        // RESPONSE DATA FORMAT
        // 2 => No Normal ACA; No Hierachical LUNs; "Standard" response data format
        data.writeByte(2);

        // ADDITIONAL LENGTH
        data.writeByte(length - data.writerIndex() - 1);

        // SCCS (SCC Supported)
        // ACC (has Access Controls Coordinator)
        // TPGS (Target Port Group Support)
        // 3PC (Third Party Copy supported)
        // Reserved
        // PROTECT (Protection supported)
        data.writeByte(0xb0);

        // Obsolete (Formerly BQUE)
        // ENCSERV (Enclosure Services supported)
        // VS
        // MULTIP (Multi Port support)
        // MCHNGR (Medium Changer support)
        // Obsolete
        // Obsolete
        // ADDR16
        data.writeByte(0x00);

        // Obsolete
        // Obsolete
        // WBUS16
        // SYNC
        // Obsolete (Formerly LINKED = Linked Command)
        // Obsolete
        // CMDQUE (Suports Command Queuing)
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

    @Override
    public String toString() {
        return "ScsiInquiryRequest [codepage=" + codepage + ", evpd=" + evpd + ", expectedLength=" + expectedLength
                + "]";
    }

}
