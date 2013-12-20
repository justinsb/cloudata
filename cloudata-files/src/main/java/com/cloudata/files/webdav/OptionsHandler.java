package com.cloudata.files.webdav;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponseStatus;

import com.cloudata.files.fs.FsPath;

public class OptionsHandler extends MethodHandler {

    public OptionsHandler(WebdavRequestHandler requestHandler) {
        super(requestHandler);
    }

    @Override
    protected HttpObject doAction(FsPath fsPath) throws Exception {
        // TODO: Do we need to include Etag on an object
        // TODO: Do we care about fsPath?

        FullHttpResponse response = buildFullResponse(HttpResponseStatus.OK);

        response.headers().set(HttpHeaders.Names.ACCEPT_RANGES, HttpHeaders.Values.BYTES);

        // Is this path specific??
        // response.setHeader(HttpHeaders.Names.ALLOW, "DELETE,MKCOL,PROPFIND,PROPPATCH,OPTIONS,MOVE,PUT");
        response.headers().set(HttpHeaders.Names.ALLOW, "DELETE,MKCOL,PROPFIND,PROPPATCH,OPTIONS,MOVE,PUT,LOCK,UNLOCK");

        return response;
    }

}
