package com.cloudata.keyvalue.redis;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;

public class RedisSession {

    private static final AttributeKey<RedisSession> KEY = AttributeKey.valueOf(RedisSession.class.getSimpleName());

    private final RedisServer server;

    private int keyspaceId;

    private ByteString keyspaceIdPrefix;

    public RedisSession(RedisServer server) {
        this.server = server;
        setKeyspaceId(0);
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

    public void setKeyspaceId(int keyspaceId) {
        Preconditions.checkArgument(keyspaceId >= 0);
        this.keyspaceId = keyspaceId;

        if (keyspaceId < 128) {
            assert 1 == CodedOutputStream.computeInt32SizeNoTag(keyspaceId);
            byte[] data = new byte[1];
            data[0] = (byte) keyspaceId;
            this.keyspaceIdPrefix = ByteString.copyFrom(data);
        } else {
            try {
                byte[] data = new byte[CodedOutputStream.computeInt32SizeNoTag(keyspaceId)];
                CodedOutputStream c = CodedOutputStream.newInstance(data);
                c.writeInt32NoTag(keyspaceId);
                c.flush();

                this.keyspaceIdPrefix = ByteString.copyFrom(data);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public int getKeyspaceId() {
        return keyspaceId;
    }

    public ByteString mapToKey(ByteString key) {
        return keyspaceIdPrefix.concat(key);
    }

}
