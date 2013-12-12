package com.cloudata.keyvalue.redis;

public class PingCommand implements RedisCommand {

    @Override
    public RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException {
        return StatusRedisResponse.PONG;
    }

}
