package com.cloudata.keyvalue.redis.commands;

import com.cloudata.keyvalue.operation.DeleteOperation;
import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.RedisSession;
import com.cloudata.keyvalue.redis.response.IntegerRedisResponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;
import com.google.protobuf.ByteString;

public class DelCommand implements RedisCommand {
    @Override
    public RedisResponse execute(RedisServer server, RedisSession session, RedisRequest command) throws RedisException {
        int count = 0;

        // TODO: Put into one transaction
        for (int i = 1; i < command.getArgc(); i++) {
            ByteString key = command.getByteString(i);

            ByteString qualifiedKey = session.getKeyspace().mapToKey(key);

            DeleteOperation operation = DeleteOperation.build(server.getStoreId(), qualifiedKey);
            Integer deleted = server.doAction(operation);

            count += deleted.intValue();
        }

        return IntegerRedisResponse.valueOf(count);
    }
}
