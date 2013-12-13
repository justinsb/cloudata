package com.cloudata.keyvalue.redis.commands;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.btree.operation.KeyOperation;
import com.cloudata.keyvalue.btree.operation.SetOperation;
import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.response.ErrorRedisReponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;
import com.cloudata.keyvalue.redis.response.StatusRedisResponse;
import com.google.protobuf.ByteString;

public class SetCommand implements RedisCommand {
    @Override
    public RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException {
        int argc = command.getArgc();
        if (argc != 3) {
            return ErrorRedisReponse.NOT_IMPLEMENTED;
        }

        ByteString key = command.getByteString(1);
        byte[] value = command.get(2);

        KeyOperation operation = new SetOperation(ByteBuffer.wrap(value));
        server.doAction(key, operation);

        return StatusRedisResponse.OK;
    }
}
