package com.cloudata.blockstore.iscsi;

public class RedisException extends Exception {

    private static final long serialVersionUID = 1L;

    public RedisException(String message, Throwable cause) {
        super(message, cause);
    }

    public RedisException(String message) {
        super(message);
    }

}
