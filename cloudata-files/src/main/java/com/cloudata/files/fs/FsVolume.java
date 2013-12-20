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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (key ^ (key >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        FsVolume other = (FsVolume) obj;
        if (key != other.key) {
            return false;
        }
        return true;
    }

}
