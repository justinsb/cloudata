package com.cloudata.keyvalue.redis.commands;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.btree.operation.Value;
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

        Value value = server.get(key);
        if (value == null) {
            return BulkRedisResponse.NIL_REPLY;
        }

        ByteBuffer bytes = value.asBytes();
        return new BulkRedisResponse(bytes);

        // switch (value.getValueType()) {
        // case ValueType.INT64: {
        // long v = value.asLong();
        // ByteBuffer valueBuffer = ByteBuffer.wrap(Long.toString(v).getBytes());
        // return new BulkRedisResponse(valueBuffer);
        // }
        //
        // case ValueType.RAW: {
        // ByteBuffer bytes = Values.asBytes(value);
        // return new BulkRedisResponse(bytes);
        // }
        //
        // default:
        // throw new IllegalStateException();
        // }
    }
}
