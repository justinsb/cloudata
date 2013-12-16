package com.cloudata.keyvalue.redis.commands;

import com.cloudata.keyvalue.btree.operation.SetOperation;
import com.cloudata.keyvalue.btree.operation.Value;
import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.RedisSession;
import com.cloudata.keyvalue.redis.response.ErrorRedisReponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;
import com.cloudata.keyvalue.redis.response.StatusRedisResponse;
import com.google.protobuf.ByteString;

public class SetCommand implements RedisCommand {
    @Override
    public RedisResponse execute(RedisServer server, RedisSession session, RedisRequest command) throws RedisException {
        int argc = command.getArgc();
        if (argc != 3) {
            return ErrorRedisReponse.NOT_IMPLEMENTED;
        }

        ByteString key = session.mapToKey(command.getByteString(1));
        ByteString value = command.getByteString(2);

        SetOperation operation = new SetOperation(Value.fromRawBytes(value));
        server.doAction(key, operation);

        return StatusRedisResponse.OK;
    }
}
