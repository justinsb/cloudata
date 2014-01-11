package com.cloudata.clients.structured.cloudata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.clients.structured.AlreadyExistsException;
import com.cloudata.clients.structured.StructuredService;
import com.cloudata.clients.structured.StructuredStore;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class CloudataStructuredService implements StructuredService {

    private static final Logger log = LoggerFactory.getLogger(CloudataStructuredService.class);

    static final Random random = new Random();

    private static final int SPACE_STORES = 0;

    final CloudataStructuredClient client;

    public CloudataStructuredService(CloudataStructuredClient client) {
        this.client = client;
    }

    @Override
    public StructuredStore get(ByteString id) {
        int idSize = id.size();
        Preconditions.checkArgument(idSize == 8);

        long storeId = id.asReadOnlyByteBuffer().getLong();
        Preconditions.checkArgument(storeId > 0); // 0 is reserved for the "list of stores"
        return new CloudataStructuredStore(client, storeId);
    }

    @Override
    public ByteString allocate() throws IOException {
        CloudataStructuredStore systemStore = new CloudataStructuredStore(client, 0);

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

            try {
                systemStore.create(SPACE_STORES, key, ByteString.EMPTY);
            } catch (AlreadyExistsException e) {
                log.debug("Retrying after conflict creating space", e);
                continue;
            }
        }
    }

    @Override
    public void delete(ByteString id) {
        throw new UnsupportedOperationException();
    }
}
