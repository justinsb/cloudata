package com.cloudata.keyvalue.redis.commands;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.btree.ByteBuffers;
import com.cloudata.keyvalue.btree.operation.IncrementOperation;
import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.response.IntegerRedisResponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;

public class IncrByCommand implements RedisCommand {
    private static final Logger log = LoggerFactory.getLogger(IncrByCommand.class);

    @Override
    public RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException {
        long delta = command.getLong(2);

        return execute(server, command, delta);
    }

    protected IntegerRedisResponse execute(RedisServer server, RedisRequest command, long deltaLong) {
        byte[] key = command.get(1);

        IncrementOperation operation = new IncrementOperation(deltaLong);
        ByteBuffer value = (ByteBuffer) server.getKeyValueStore().doAction(ByteBuffer.wrap(key), operation);

        long v = ByteBuffers.parseLong(value);
        return new IntegerRedisResponse(v);
    }
}
