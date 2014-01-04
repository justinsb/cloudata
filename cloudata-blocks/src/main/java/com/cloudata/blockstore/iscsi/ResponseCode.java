package com.cloudata.blockstore.iscsi;

public enum ResponseCode {
    CompletedAtTarget(0x00);

    final byte code;

    private ResponseCode(int code) {
        this.code = (byte) code;
    }

}