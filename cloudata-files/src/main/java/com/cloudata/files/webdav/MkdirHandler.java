package com.cloudata.files.webdav;

import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponseStatus;

import com.cloudata.files.FilesModel.InodeData;
import com.cloudata.files.fs.FsClient;
import com.cloudata.files.fs.FsPath;
import com.cloudata.files.fs.Inode;
import com.google.protobuf.ByteString;

public class MkdirHandler extends MethodHandler {

    public MkdirHandler(WebdavRequestHandler requestHandler) {
        super(requestHandler);
    }

    @Override
    protected boolean shouldResolveParent() {
        return true;
    }

    @Override
    protected HttpObject doAction(FsPath parentPath) throws Exception {
        WebdavRequest request = getRequest();
        FsClient fsClient = getFsClient();

        if (!parentPath.isFolder()) {
            throw new WebdavResponseException(HttpResponseStatus.CONFLICT);
        }

        if (request.hasContent()) {
            throw new UnsupportedOperationException();
        }

        String path = request.getUri();
        String name = Urls.getLastPathComponent(path);

        InodeData.Builder inode = InodeData.newBuilder();

        long now = System.currentTimeMillis();

        inode.setCreateTime(now);
        inode.setModifiedTime(now);

        int mode = Inode.S_IFDIR;
        inode.setMode(mode);

        boolean overwrite = false;
        fsClient.createNewFolder(parentPath, ByteString.copyFromUtf8(name), inode, overwrite);

        return buildFullResponse(HttpResponseStatus.CREATED);
    }

}
