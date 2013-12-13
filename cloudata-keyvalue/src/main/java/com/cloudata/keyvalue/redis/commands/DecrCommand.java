package com.cloudata.keyvalue.redis.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.response.RedisResponse;

public class DecrCommand extends IncrByCommand {
    private static final Logger log = LoggerFactory.getLogger(DecrCommand.class);

    @Override
    public RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException {
        return execute(server, command, -1);
    }
}
