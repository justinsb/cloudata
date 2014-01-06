package com.cloudata.keyvalue.redis.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.KeyValueProtocol.ActionResponse;
import com.cloudata.keyvalue.KeyValueProtocol.ResponseEntry;
import com.cloudata.keyvalue.operation.IncrementOperation;
import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.RedisSession;
import com.cloudata.keyvalue.redis.response.IntegerRedisResponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;
import com.cloudata.values.Value;
import com.google.protobuf.ByteString;

public class IncrByCommand implements RedisCommand {
    private static final Logger log = LoggerFactory.getLogger(IncrByCommand.class);

    @Override
    public RedisResponse execute(RedisServer server, RedisSession session, RedisRequest command) throws RedisException {
        long delta = command.getLong(2);

        return execute(server, session, command, delta);
    }

    protected IntegerRedisResponse execute(RedisServer server, RedisSession session, RedisRequest command,
            long deltaLong) throws RedisException {
        ByteString key = command.getByteString(1);
        // ByteString qualifiedKey = session.getKeyspace().mapToKey(key);

        IncrementOperation operation = IncrementOperation.build(server.getStoreId(), session.getKeyspace(), key,
                deltaLong);
        ActionResponse response = server.doAction(operation);
        if (response.getEntryCount() == 0) {
            throw new IllegalStateException();
        }
        ResponseEntry entry = response.getEntry(0);
        Value newValue = Value.fromRawBytes(entry.getValue());
        return IntegerRedisResponse.valueOf(newValue.asLong());
    }
}
