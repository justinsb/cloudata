package com.cloudata.keyvalue.redis.commands;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.btree.operation.DeleteOperation;
import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.response.IntegerRedisResponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;

public class DelCommand implements RedisCommand {
    @Override
    public RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException {
        int count = 0;

        // TODO: Put into one transaction
        for (int i = 1; i < command.getArgc(); i++) {
            byte[] key = command.get(i);

            DeleteOperation operation = new DeleteOperation();
            server.getKeyValueStore().doAction(ByteBuffer.wrap(key), operation);

            count += operation.getDeleteCount();
        }

        return IntegerRedisResponse.valueOf(count);
    }
}
