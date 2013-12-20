package com.cloudata.files.webdav;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.files.FilesModel.InodeData;
import com.cloudata.files.fs.FsPath;
import com.cloudata.files.webdav.chunks.ChunkAccumulator;
import com.cloudata.files.webdav.chunks.SimpleChunkAccumulator;
import com.google.common.io.ByteSource;
import com.google.protobuf.ByteString;

public class PutHandler extends MethodHandler {

    private static final Logger log = LoggerFactory.getLogger(PutHandler.class);

    public PutHandler(WebdavRequestHandler requestHandler) {
        super(requestHandler);
    }

    @Override
    protected boolean shouldResolveParent() {
        return true;
    }

    @Override
    public HttpObject doAction(FsPath parentPath) throws Exception {
        if (!parentPath.isFolder()) {
            throw new WebdavResponseException(HttpResponseStatus.CONFLICT);
        }

        String path = getUri();
        String newName = Urls.getLastPathComponent(path, true);

        log.debug("PUT {} {}", parentPath, newName);

        // TODO: Always overwrite??
        boolean overwrite = true;

        ChunkAccumulator content = getContent();

        if (content == null) {
            // Windows like to create an empty file first...
            content = new SimpleChunkAccumulator();
        }

        InodeData.Builder inode = InodeData.newBuilder();

        long now = System.currentTimeMillis();

        inode.setCreateTime(now);
        inode.setModifiedTime(now);

        if (content instanceof SimpleChunkAccumulator) {
            SimpleChunkAccumulator simpleContent = (SimpleChunkAccumulator) content;

            ByteSource source = simpleContent.getByteSource();
            getFsClient().createNewFile(parentPath, ByteString.copyFromUtf8(newName), inode, source, overwrite);
        } else {
            throw new IllegalStateException();
        }

        FullHttpResponse response = buildFullResponse(HttpResponseStatus.CREATED);

        return response;
    }

}
