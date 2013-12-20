package com.cloudata.files.webdav;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponseStatus;

import com.cloudata.files.fs.FsPath;
import com.cloudata.files.locks.LockService;
import com.google.common.base.Strings;

public class UnlockHandler extends MethodHandler {

    public UnlockHandler(WebdavRequestHandler requestHandler) {
        super(requestHandler);
    }

    @Override
    protected HttpObject doAction(FsPath fsPath) throws Exception {
        LockService lockService = getLockService();
        WebdavRequest request = getRequest();

        String lockToken = request.getHeader("Lock-Token");
        if (Strings.isNullOrEmpty(lockToken)) {
            throw new IllegalArgumentException("Lock-Token is required");
        }

        lockToken = lockToken.trim();

        if (lockToken.startsWith("<")) {
            lockToken = lockToken.substring(1).trim();
        }

        if (lockToken.startsWith(LockHandler.LOCK_PREFIX)) {
            lockToken = lockToken.substring(LockHandler.LOCK_PREFIX.length()).trim();
        }

        if (lockToken.endsWith(">")) {
            lockToken = lockToken.substring(0, lockToken.length() - 1);
        }
        String lockSubject = buildLockSubject(request);

        boolean unlocked = lockService.unlock(lockSubject, lockToken);
        if (!unlocked) {
            throw new IllegalArgumentException("Lock not found");
        }

        FullHttpResponse response = buildFullResponse(HttpResponseStatus.NO_CONTENT);
        return response;
    }
}
