package com.cloudata.files.webdav.chunks;

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;

public interface ChunkAccumulator extends Closeable {

    void append(ByteBuf chunkContent) throws IOException;

    void end() throws IOException;

    boolean isEmpty();

}
