package com.cloudata.keyvalue.redis.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.Keyspace;
import com.cloudata.keyvalue.redis.RedisException;
import com.cloudata.keyvalue.redis.RedisRequest;
import com.cloudata.keyvalue.redis.RedisServer;
import com.cloudata.keyvalue.redis.RedisSession;
import com.cloudata.keyvalue.redis.response.ErrorRedisReponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;
import com.cloudata.keyvalue.redis.response.StatusRedisResponse;
import com.google.common.primitives.Ints;

public class SelectCommand implements RedisCommand {
    private static final Logger log = LoggerFactory.getLogger(SelectCommand.class);

    @Override
    public RedisResponse execute(RedisServer server, RedisSession session, RedisRequest command) throws RedisException {
        long keyspaceId = command.getLong(1);

        if (keyspaceId < 0 || keyspaceId >= Integer.MAX_VALUE) {
            return new ErrorRedisReponse("invalid DB index");
        }

        session.setKeyspace(Keyspace.build(Ints.checkedCast(keyspaceId)));
        return StatusRedisResponse.OK;
    }
}
