package com.cloudata.files.webdav;

import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.FileNotFoundException;
import java.net.URI;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.files.fs.FsCredentials;
import com.cloudata.files.fs.FsPath;
import com.cloudata.files.fs.FsVolume;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;

public class MoveHandler extends MethodHandler {

    private static final Logger log = LoggerFactory.getLogger(MoveHandler.class);

    public MoveHandler(WebdavRequestHandler requestHandler) {
        super(requestHandler);
    }

    @Override
    protected HttpObject doAction(FsPath fsPath) throws Exception {
        WebdavRequest request = getRequest();

        String destination = request.getHeader("Destination");
        if (Strings.isNullOrEmpty(destination)) {
            throw new IllegalArgumentException();
        }

        String newName;
        FsPath newParentFsPath;

        {
            URI uri = URI.create(destination);
            String path = uri.getPath();

            String parentPath = Urls.getParentPath(path);
            List<String> pathTokens = Urls.pathToTokens(parentPath);
            newName = Urls.getLastPathComponent(path, true);

            FsVolume volume = fsPath.getVolume();
            if (!volume.equals(FsVolume.fromHost(uri.getHost()))) {
                log.error("Move attempted between different volumes: {} {}", getUri(), uri);
                throw new IllegalArgumentException();
            }
            FsCredentials credentials = request.getCredentials();

            newParentFsPath = getFsClient().resolve(volume, credentials, pathTokens);
        }

        if (newParentFsPath == null) {
            throw new FileNotFoundException();
        }

        getFsClient().move(fsPath, newParentFsPath, ByteString.copyFromUtf8(newName));

        return buildFullResponse(HttpResponseStatus.CREATED);
    }

}
