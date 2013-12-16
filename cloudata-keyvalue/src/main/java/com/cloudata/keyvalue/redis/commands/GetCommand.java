package com.cloudata.keyvalue.redis.commands;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.RedisSession;
import com.cloudata.keyvalue.redis.response.BulkRedisResponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;
import com.google.protobuf.ByteString;

public class GetCommand implements RedisCommand {
    private static final Logger log = LoggerFactory.getLogger(GetCommand.class);

    @Override
    public RedisResponse execute(RedisServer server, RedisSession session, RedisRequest command) throws RedisException {
        ByteString key = session.mapToKey(command.getByteString(1));

        ByteBuffer value = server.get(key);

        if (value == null) {
            return BulkRedisResponse.NIL_REPLY;
        }

        return new BulkRedisResponse(value);
    }
}
