package com.cloudata.clients.keyvalue.redis;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.cloudata.clients.keyvalue.KeyValueService;
import com.cloudata.clients.keyvalue.KeyValueStore;
import com.cloudata.clients.keyvalue.PrefixKeyValueStore;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class RedisKeyValueService implements KeyValueService {

    final RedisKeyValueStore redis;

    public RedisKeyValueService(RedisKeyValueStore redis) {
        this.redis = redis;
    }

    @Override
    public KeyValueStore get(ByteString id) {
        int idSize = id.size();
        // 0 and >128 are reserved
        Preconditions.checkArgument(idSize > 0);
        Preconditions.checkArgument(idSize <= Byte.MAX_VALUE);

        ByteString length = ByteString.copyFrom(new byte[] { (byte) (idSize & 0xff) });
        ByteString prefix = length.concat(id);

        return PrefixKeyValueStore.create(redis, prefix);
    }

    @Override
    public ByteString allocate() {
        // TODO: Verify not already allocated; allocate more densely?
        UUID uuid = UUID.randomUUID();

        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getLeastSignificantBits());
        b.putLong(uuid.getMostSignificantBits());
        b.flip();

        return ByteString.copyFrom(b);
    }

    @Override
    public void delete(ByteString id) {
        throw new UnsupportedOperationException();
    }
}
