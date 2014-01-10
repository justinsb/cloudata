package com.cloudata.clients.structured;

import com.google.protobuf.ByteString;

public interface StructuredService {
    StructuredStore get(ByteString id);

    ByteString allocate();

    void delete(ByteString id);
}
