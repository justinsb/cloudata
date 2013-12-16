package com.cloudata.keyvalue.redis.commands;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.RedisSession;
import com.cloudata.keyvalue.redis.response.IntegerRedisResponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;

public class ExistsCommand implements RedisCommand {
    private static final Logger log = LoggerFactory.getLogger(ExistsCommand.class);

    @Override
    public RedisResponse execute(RedisServer server, RedisSession session, RedisRequest command) throws RedisException {
        byte[] key = command.get(1);

        ByteBuffer value = server.get(ByteBuffer.wrap(key));

        if (value == null) {
            return IntegerRedisResponse.ZERO;
        } else {
            return IntegerRedisResponse.ONE;
        }
    }
}
