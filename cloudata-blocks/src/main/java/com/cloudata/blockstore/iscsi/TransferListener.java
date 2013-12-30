package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

public interface TransferListener {

    void gotData(int bufferOffset, ByteBuf data);

    void endOfData();

}
