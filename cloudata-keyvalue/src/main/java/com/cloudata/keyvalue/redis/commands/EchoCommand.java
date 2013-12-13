package com.cloudata.keyvalue.redis.commands;

import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.response.BulkRedisResponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;

public class EchoCommand implements RedisCommand {
    @Override
    public RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException {
        byte[] data = command.get(1);
        return new BulkRedisResponse(data);
    }
}
