package com.cloudata.clients.structured;

import java.io.IOException;

import com.google.protobuf.ByteString;

public interface StructuredService {
    StructuredStore get(ByteString id);

    ByteString allocate() throws IOException;

    void delete(ByteString id);
}
