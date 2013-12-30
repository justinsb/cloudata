package com.cloudata.blockstore.iscsi;

public enum Stage {
    Negotiation(0x01), FullFeaturePhase(0x03);

    final byte code;

    private Stage(int code) {
        this.code = (byte) code;
    }

}
