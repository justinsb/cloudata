package com.cloudata.files.webdav;

import io.netty.handler.codec.http.HttpResponseStatus;

public class WebdavResponseException extends Exception {
    private static final long serialVersionUID = 1L;

    final boolean fatal;

    final HttpResponseStatus status;

    public WebdavResponseException(HttpResponseStatus status, String message, boolean fatal) {
        super(message);
        this.status = status;
        this.fatal = fatal;
    }

    public WebdavResponseException(HttpResponseStatus status) {
        this.status = status;
        this.fatal = status.code() >= 500;
    }

    public HttpResponseStatus getStatus() {
        return status;
    }

    public boolean isFatal() {
        return fatal;
    }

}
