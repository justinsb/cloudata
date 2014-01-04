package com.cloudata.blockstore;

import java.io.IOException;

import com.google.protobuf.ByteString;

public interface VolumeProvider {

    Volume get(ByteString name) throws IOException;

    Volume ensureVolume(ByteString name, int sizeGb) throws IOException;
}
