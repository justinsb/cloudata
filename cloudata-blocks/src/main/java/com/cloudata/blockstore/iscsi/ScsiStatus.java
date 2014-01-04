package com.cloudata.blockstore.iscsi;

public enum ScsiStatus {
    Good(0x00);

    final byte code;

    private ScsiStatus(int code) {
        this.code = (byte) code;
    }

}