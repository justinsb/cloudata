package com.cloudata.keyvalue.redis.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.btree.operation.Value;
import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.RedisSession;
import com.cloudata.keyvalue.redis.response.IntegerRedisResponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;
import com.google.protobuf.ByteString;

public class ExistsCommand implements RedisCommand {
    private static final Logger log = LoggerFactory.getLogger(ExistsCommand.class);

    @Override
    public RedisResponse execute(RedisServer server, RedisSession session, RedisRequest command) throws RedisException {
        ByteString key = command.getByteString(1);

        Value value = server.get(session.getKeyspace(), key);

        if (value == null) {
            return IntegerRedisResponse.ZERO;
        } else {
            return IntegerRedisResponse.ONE;
        }
    }
}
