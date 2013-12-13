package com.cloudata.keyvalue.redis.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.response.RedisResponse;

public class DecrByCommand extends IncrByCommand {
    private static final Logger log = LoggerFactory.getLogger(DecrByCommand.class);

    @Override
    public RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException {
        long delta = command.getLong(2);
        delta = -delta;
        return execute(server, command, delta);
    }

}
