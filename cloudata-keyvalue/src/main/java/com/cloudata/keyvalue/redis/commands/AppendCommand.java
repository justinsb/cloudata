package com.cloudata.keyvalue.redis.commands;

import com.cloudata.keyvalue.operation.AppendOperation;
import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.RedisSession;
import com.cloudata.keyvalue.redis.response.ErrorRedisReponse;
import com.cloudata.keyvalue.redis.response.IntegerRedisResponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;
import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class AppendCommand implements RedisCommand {
    @Override
    public RedisResponse execute(RedisServer server, RedisSession session, RedisRequest command) throws RedisException {
        int argc = command.getArgc();
        if (argc != 3) {
            return ErrorRedisReponse.NOT_IMPLEMENTED;
        }

        ByteString key = command.getByteString(1);
        ByteString qualifiedKey = session.getKeyspace().mapToKey(key);

        ByteString value = command.getByteString(2);

        AppendOperation operation = AppendOperation.build(server.getStoreId(), qualifiedKey, Value.fromRawBytes(value));
        Number ret = server.doAction(operation);

        return IntegerRedisResponse.valueOf(ret.longValue());
    }
}
