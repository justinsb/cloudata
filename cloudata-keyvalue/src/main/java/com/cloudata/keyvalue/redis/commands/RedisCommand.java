package com.cloudata.keyvalue.redis.commands;

import org.robotninjas.barge.RaftException;

import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.response.RedisResponse;

public interface RedisCommand {
    RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException, InterruptedException,
            RaftException;
}