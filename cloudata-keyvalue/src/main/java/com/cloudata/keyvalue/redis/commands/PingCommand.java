package com.cloudata.keyvalue.redis.commands;

import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.response.RedisResponse;
import com.cloudata.keyvalue.redis.response.StatusRedisResponse;

public class PingCommand implements RedisCommand {

    @Override
    public RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException {
        return StatusRedisResponse.PONG;
    }

}
