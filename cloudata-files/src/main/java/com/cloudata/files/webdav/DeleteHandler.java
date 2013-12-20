package com.cloudata.files.webdav;

import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponseStatus;

import com.cloudata.files.fs.FsClient;
import com.cloudata.files.fs.FsPath;

public class DeleteHandler extends MethodHandler {

    public DeleteHandler(WebdavRequestHandler requestHandler) {
        super(requestHandler);
    }

    @Override
    protected HttpObject doAction(FsPath fsPath) throws Exception {
        WebdavRequest request = getRequest();
        FsClient fsClient = getFsClient();

        int depth = request.getDepth();

        if (fsPath.isFolder()) {
            if (depth != WebdavRequest.DEPTH_INFINITY) {
                throw new WebdavResponseException(HttpResponseStatus.CONFLICT);
            }
        }

        if (!fsClient.delete(fsPath)) {
            throw new WebdavResponseException(HttpResponseStatus.CONFLICT);
        }

        return buildFullResponse(HttpResponseStatus.NO_CONTENT);
    }
}
