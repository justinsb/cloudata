package com.cloudata.keyvalue.redis;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.keyvalue.redis.commands.EchoCommand;
import com.cloudata.keyvalue.redis.commands.GetCommand;
import com.cloudata.keyvalue.redis.commands.IncrByCommand;
import com.cloudata.keyvalue.redis.commands.IncrCommand;
import com.cloudata.keyvalue.redis.commands.PingCommand;
import com.cloudata.keyvalue.redis.commands.QuitCommand;
import com.cloudata.keyvalue.redis.commands.RedisCommand;
import com.cloudata.keyvalue.redis.commands.SetCommand;
import com.cloudata.keyvalue.redis.response.ErrorRedisReponse;
import com.cloudata.keyvalue.redis.response.InlineRedisResponse;
import com.cloudata.keyvalue.redis.response.RedisResponse;
import com.cloudata.keyvalue.redis.response.StatusRedisResponse;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

/**
 * Handle decoded commands
 */
@ChannelHandler.Sharable
public class RedisRequestHandler extends SimpleChannelInboundHandler<RedisRequest> {
    private static final Logger log = LoggerFactory.getLogger(RedisRequestHandler.class);

    static final Map<ByteString, RedisCommand> methods = Maps.newHashMap();

    static {
        addMethod("echo", new EchoCommand());
        addMethod("ping", new PingCommand());
        addMethod("quit", new QuitCommand());

        addMethod("set", new SetCommand());
        addMethod("get", new GetCommand());

        addMethod("incr", new IncrCommand());
        addMethod("incrby", new IncrByCommand());
    }

    static void addMethod(String name, RedisCommand action) {
        methods.put(ByteString.copyFromUtf8(name.toLowerCase()), action);
    }

    private final RedisServer server;

    public RedisRequestHandler(RedisServer server) {
        this.server = server;
        // Class<? extends RedisServer> aClass = rs.getClass();
        // for (final Method method : aClass.getMethods()) {
        // final Class<?>[] types = method.getParameterTypes();
        // methods.put(new BytesKey(method.getName().getBytes()), new Wrapper() {
        // @Override
        // public Reply execute(Command command) throws RedisException {
        // Object[] objects = new Object[types.length];
        // try {
        // command.toArguments(objects, types);
        // return (Reply) method.invoke(rs, objects);
        // } catch (IllegalAccessException e) {
        // throw new RedisException("Invalid server implementation");
        // } catch (InvocationTargetException e) {
        // Throwable te = e.getTargetException();
        // if (!(te instanceof RedisException)) {
        // te.printStackTrace();
        // }
        // return new ErrorReply("ERR " + te.getMessage());
        // } catch (Exception e) {
        // return new ErrorReply("ERR " + e.getMessage());
        // }
        // }
        // });
        // }
    }

    private static final byte LOWER_DIFF = 'a' - 'A';

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RedisRequest msg) throws Exception {
        byte[] name = msg.getName();

        // Simple force lower-case
        for (int i = 0; i < name.length; i++) {
            byte b = name[i];
            if (b >= 'A' && b <= 'Z') {
                name[i] = (byte) (b + LOWER_DIFF);
            }
        }

        log.debug("Executing command: {}", msg);

        RedisCommand command = methods.get(ByteString.copyFrom(name));
        RedisResponse reply;
        if (command == null) {
            reply = new ErrorRedisReponse("unknown command '" + new String(name, Charsets.US_ASCII) + "'");
        } else {
            try {
                reply = command.execute(server, msg);
            } catch (Throwable t) {
                log.warn("Error executing command", t);
                reply = ErrorRedisReponse.INTERNAL_ERROR;
            }
        }
        if (reply == StatusRedisResponse.QUIT) {
            // TODO: Pipelined responses? Do we need to flush?
            ctx.close();
        } else {
            if (msg.isInline()) {
                reply = new InlineRedisResponse(reply);
            }
            // if (reply == null) {
            // reply = ErrorRedisReponse.NOT_IMPLEMENTED;
            // }
            ctx.write(reply);
        }
    }
}