package com.cloudata.keyvalue.redis;

interface RedisCommand {
    RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException;
}