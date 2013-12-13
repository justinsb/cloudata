package com.cloudata.keyvalue.redis.commands;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.btree.operation.AppendOperation;
import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.response.ErrorRedisReponse;
import com.cloudata.keyvalue.redis.response.IntegerRedisResponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;

public class AppendCommand implements RedisCommand {
    @Override
    public RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException {
        int argc = command.getArgc();
        if (argc != 3) {
            return ErrorRedisReponse.NOT_IMPLEMENTED;
        }

        byte[] key = command.get(1);
        byte[] value = command.get(2);

        AppendOperation operation = new AppendOperation(ByteBuffer.wrap(value));
        server.getKeyValueStore().doAction(ByteBuffer.wrap(key), operation);

        return IntegerRedisResponse.valueOf(operation.getNewLength());
    }
}
