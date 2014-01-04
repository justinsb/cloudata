package com.cloudata.clients.keyvalue;

import com.google.protobuf.ByteString;

public interface KeyValueService {
    public KeyValueStore get(ByteString id);

    public ByteString allocate();

    public void delete(ByteString id);
}
