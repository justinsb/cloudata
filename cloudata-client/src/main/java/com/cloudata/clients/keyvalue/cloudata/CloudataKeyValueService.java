package com.cloudata.clients.keyvalue.cloudata;

import java.nio.ByteBuffer;
import java.util.Random;

import com.cloudata.clients.keyvalue.IfNotExists;
import com.cloudata.clients.keyvalue.KeyValueService;
import com.cloudata.clients.keyvalue.KeyValueStore;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class CloudataKeyValueService implements KeyValueService {
    static final Random random = new Random();
    private static final int SPACE_STORES = 0;

    final CloudataKeyValueClient client;

    public CloudataKeyValueService(CloudataKeyValueClient client) {
        this.client = client;
    }

    @Override
    public KeyValueStore get(ByteString id) {
        int idSize = id.size();
        Preconditions.checkArgument(idSize == 8);

        long storeId = id.asReadOnlyByteBuffer().getLong();
        Preconditions.checkArgument(storeId > 0); // 0 is reserved for the "list of stores"
        return new CloudataKeyValueStore(client, storeId);
    }

    @Override
    public ByteString allocate() {
        CloudataKeyValueStore systemStore = new CloudataKeyValueStore(client, 0);

        long storeId;
        while (true) {
            synchronized (random) {
                storeId = random.nextLong();
            }

            if (storeId <= 0) {
                storeId = -storeId;
                if (storeId == 0) {
                    continue;
                }
            }

            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong(storeId);
            buffer.flip();
            ByteString key = ByteString.copyFrom(buffer);

            boolean success = systemStore.putSync(SPACE_STORES, key, ByteString.EMPTY, IfNotExists.INSTANCE);
            if (success) {
                return key;
            }
        }
    }

    @Override
    public void delete(ByteString id) {
        throw new UnsupportedOperationException();
    }
}
