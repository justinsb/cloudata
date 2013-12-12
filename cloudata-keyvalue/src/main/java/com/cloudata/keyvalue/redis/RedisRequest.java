package com.cloudata.keyvalue.redis;

public class RedisRequest {
    final byte[][] command;
    final boolean inline;

    public RedisRequest(byte[] inlineCommand, boolean inline) {
        if (!inline) {
            throw new IllegalArgumentException();
        }
        this.command = new byte[][] { inlineCommand };
        this.inline = inline;
    }

    public RedisRequest(byte[][] command) {
        this.command = command;
        this.inline = false;
    }

    public boolean isInline() {
        return inline;
    }

    public byte[] getName() {
        return command[0];
    }

}
