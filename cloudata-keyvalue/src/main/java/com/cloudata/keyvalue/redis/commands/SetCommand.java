package com.cloudata.keyvalue.redis.commands;

import java.nio.ByteBuffer;

import com.cloudata.keyvalue.KeyValueProto.KvAction;
import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.response.ErrorRedisReponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;
import com.cloudata.keyvalue.redis.response.StatusRedisResponse;

public class SetCommand implements RedisCommand {
    @Override
    public RedisResponse execute(RedisServer server, RedisRequest command) throws RedisException {
        int argc = command.getArgc();
        if (argc != 3) {
            return ErrorRedisReponse.NOT_IMPLEMENTED;
        }

        byte[] key = command.get(1);
        byte[] value = command.get(2);

        server.getKeyValueStore().doAction(KvAction.SET, ByteBuffer.wrap(key), ByteBuffer.wrap(value));

        return StatusRedisResponse.OK;
    }
}
