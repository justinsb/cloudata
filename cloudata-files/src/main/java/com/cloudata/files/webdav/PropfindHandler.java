package com.cloudata.files.webdav;

import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.files.fs.FsClient.DirEntry;
import com.cloudata.files.fs.FsPath;
import com.cloudata.files.fs.Inode;
import com.cloudata.files.webdav.model.PropfindRequest;
import com.cloudata.files.webdav.model.PropfindResponse;
import com.cloudata.files.webdav.model.WebdavProperty;
import com.cloudata.files.webdav.model.WebdavPropertyList;
import com.cloudata.files.webdav.model.WebdavPropertyValue;

public class PropfindHandler extends MethodHandler {

    private static final Logger log = LoggerFactory.getLogger(PropfindHandler.class);

    final DateFormatter dateFormatter;

    public PropfindHandler(WebdavRequestHandler handler) {
        super(handler);

        this.dateFormatter = DateFormatter.get();
    }

    @Override
    public HttpObject doAction(FsPath fsPath) throws Exception {
        WebdavRequest request = getRequest();

        Iterator<DirEntry> children = null;

        int depth = request.getDepth();
        if (depth == 1 && fsPath.isFolder()) {
            children = getFsClient().listChildren(fsPath);
        }

        PropfindRequest propfindRequest;
        try {
            propfindRequest = new PropfindRequest(request);
        } catch (IOException e) {
            log.warn("Error parsing request", e);
            throw new IllegalArgumentException();
        }

        if (depth == WebdavRequest.DEPTH_INFINITY) {
            log.warn("Depth: infinity not implemented");
            throw new WebdavResponseException(HttpResponseStatus.METHOD_NOT_ALLOWED,
                    "PROPFIND with 'Depth: infinity' not supported", false);
        }

        WebdavPropertyList requestedProperties = propfindRequest.getRequestedProperties();
        PropfindResponse response = new PropfindResponse(requestedProperties);

        if (depth < 0) {
            throw new IllegalArgumentException();
        }

        String href = fsPath.getHref();
        if (depth >= 0) {
            PropfindResponse.Response responseRoot = response.addResponse(href);
            addProperties(requestedProperties, fsPath, responseRoot);
        }

        if (depth >= 1 && fsPath.isFolder()) {
            if (!href.endsWith("/")) {
                href += "/";
            }

            while (children.hasNext()) {
                DirEntry childEntry = children.next();
                String name = childEntry.getName().toStringUtf8();
                String path = href + name;

                Inode child = childEntry.readInode();

                PropfindResponse.Response childResponse = response.addResponse(path);
                addProperties(requestedProperties, child, childResponse);
            }
        }

        if (depth >= 2) {
            throw new UnsupportedOperationException();
        }

        return buildXmlResponse(HttpResponseStatus.MULTI_STATUS, response);
    }

    private void addProperties(WebdavPropertyList requestedProperties, Inode inode, PropfindResponse.Response target) {
        if (requestedProperties.contains(WebdavProperty.RESOURCETYPE)) {
            if (inode.isFolder()) {
                target.addProperty(WebdavPropertyValue.RESOURCETYPE_COLLECTION);
            }
        }

        if (requestedProperties.contains(WebdavProperty.GETLASTMODIFIED)) {
            assert inode.getModified() != 0;
            if (inode.getModified() != 0) {
                target.addProperty(WebdavProperty.GETLASTMODIFIED, dateFormatter.format(inode.getModified()));
            }
        }

        if (requestedProperties.contains(WebdavProperty.CREATIONDATE)) {
            assert inode.getCreated() != 0;

            if (inode.getCreated() != 0) {
                target.addProperty(WebdavProperty.CREATIONDATE, dateFormatter.format(inode.getCreated()));
            }
        }

        if (requestedProperties.contains(WebdavProperty.GETCONTENTLENGTH)) {
            target.addProperty(WebdavProperty.GETCONTENTLENGTH, String.valueOf(inode.getLength()));
        }

        // if (requestedProperties.contains(WebdavProperty.GETETAG)) {
        // target.addProperty(WebdavProperty.GETETAG, toEtag(inode.getHash()));
        // }
    }

}
