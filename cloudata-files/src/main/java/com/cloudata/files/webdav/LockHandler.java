package com.cloudata.files.webdav;

import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.files.fs.FsPath;
import com.cloudata.files.locks.CloudLockToken;
import com.cloudata.files.locks.LockService;
import com.cloudata.files.webdav.model.LockRequest;
import com.cloudata.files.webdav.model.LockResponse;

public class LockHandler extends MethodHandler {
    private static final Logger log = LoggerFactory.getLogger(LockHandler.class);

    public static final String LOCK_PREFIX = "opaquelocktoken:";

    public LockHandler(WebdavRequestHandler requestHandler) {
        super(requestHandler);
    }

    @Override
    protected HttpObject doAction(FsPath fsPath) throws Exception {
        LockService lockService = getLockService();
        WebdavRequest request = getRequest();

        LockRequest lockRequest;
        try {
            lockRequest = new LockRequest(request);
        } catch (IOException e) {
            log.warn("Error parsing request", e);
            throw new IllegalArgumentException();
        }

        log.debug("LockRequest: " + lockRequest);

        CloudLockToken lockToken = lockService.tryLock(request.getUri());
        if (lockToken == null) {
            throw new WebdavResponseException(HttpResponseStatus.LOCKED);
        }

        String href = LOCK_PREFIX + lockToken.getTokenId();
        LockResponse lockResponse = new LockResponse(href);
        HttpResponse response = buildXmlResponse(HttpResponseStatus.OK, lockResponse);
        response.headers().set("Lock-Token", "<" + href + ">");
        return response;
    }

}
