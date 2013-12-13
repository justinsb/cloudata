package com.cloudata.keyvalue.redis.commands;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.btree.ByteBuffers;
import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.response.RedisResponse;

public class DecrByCommand extends IncrByCommand {
    private static final Logger log = LoggerFactory.getLogger(DecrByCommand.class);

    @Override
    public RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException {
        byte[] delta = command.get(2);

        long deltaLong = ByteBuffers.parseLong(ByteBuffer.wrap(delta));
        deltaLong = -deltaLong;
        return execute(server, command, deltaLong);
    }

}
