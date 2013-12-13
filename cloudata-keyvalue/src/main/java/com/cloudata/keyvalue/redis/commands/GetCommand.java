package com.cloudata.keyvalue.redis.commands;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.response.BulkRedisResponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;

public class GetCommand implements RedisCommand {
    private static final Logger log = LoggerFactory.getLogger(GetCommand.class);

    @Override
    public RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException {
        byte[] key = command.get(1);

        ByteBuffer value = server.get(ByteBuffer.wrap(key));

        if (value == null) {
            return BulkRedisResponse.NIL_REPLY;
        }

        return new BulkRedisResponse(value);
    }
}
