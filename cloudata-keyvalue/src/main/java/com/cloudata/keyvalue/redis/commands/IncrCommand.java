package com.cloudata.keyvalue.redis.commands;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.btree.ByteBuffers;
import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.response.IntegerRedisResponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;

public class IncrCommand implements RedisCommand {
    private static final Logger log = LoggerFactory.getLogger(IncrCommand.class);

    @Override
    public RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException {
        byte[] key = command.get(1);

        ByteBuffer value = (ByteBuffer) server.getKeyValueStore().doAction(KvAction.INCREMENT, ByteBuffer.wrap(key),
                null);

        long v = ByteBuffers.parseLong(value);
        return new IntegerRedisResponse(v);
    }
}
