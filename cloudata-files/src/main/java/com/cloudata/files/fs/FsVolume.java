package com.cloudata.files.fs;

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;

public class FsVolume {
    final long key;
    final ByteString prefix;

    public FsVolume(long key) {
        this.key = key;
        this.prefix = ByteStrings.encode(key);
    }

    public static FsVolume fromHost(String host) {
        String bucket = host;
        if (Strings.isNullOrEmpty(bucket)) {
            bucket = "local";
        }

        bucket = bucket.replace(":8080", "");
        bucket = bucket.replace("127.0.0.1", "local");

        long key = 2;
        return new FsVolume(key);
    }

    public ByteString getPrefix() {
        return prefix;
    }
}
