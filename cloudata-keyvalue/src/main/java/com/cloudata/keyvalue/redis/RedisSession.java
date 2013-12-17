package com.cloudata.keyvalue.redis;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import com.cloudata.btree.Keyspace;

public class RedisSession {

    private static final AttributeKey<RedisSession> KEY = AttributeKey.valueOf(RedisSession.class.getSimpleName());

    private final RedisServer server;

    Keyspace keyspace = Keyspace.ZERO;

    public Keyspace getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(Keyspace keyspace) {
        this.keyspace = keyspace;
    }

    public RedisSession(RedisServer server) {
        this.server = server;
    }

    public static RedisSession get(RedisServer server, ChannelHandlerContext ctx) {
        Attribute<RedisSession> attribute = ctx.attr(KEY);
        while (true) {
            RedisSession redisSession = attribute.get();
            if (redisSession != null) {
                return redisSession;
            }

            redisSession = new RedisSession(server);
            if (attribute.compareAndSet(null, redisSession)) {
                return redisSession;
            }
        }
    }

}
