package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

public abstract class IscsiResponse extends AbstractReferenceCounted implements ReferenceCounted {
    public int initiatorTaskTag = 0xffffffff;
    public int statSN;
    public int expectedCommandSN;
    public int maxCommandSN;

    public boolean flagFinal = true;

    public boolean flagResidualOverflow;
    public boolean flagResidualUnderflow;
    public int residualCount;

    public abstract void encode(ByteBuf out);

    public void setResiduals(int expectedLength, int dataLength) {
        // TODO: This is similar to ScsiResponse
        this.flagResidualOverflow = expectedLength < dataLength;
        this.flagResidualUnderflow = expectedLength > dataLength;
        this.residualCount = Math.abs(expectedLength - dataLength);
    }
}
