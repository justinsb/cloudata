package com.cloudata.files.webdav;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.stream.ChunkedInput;

import java.text.ParseException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.files.fs.FsFile;
import com.cloudata.files.fs.FsPath;
import com.google.common.base.Strings;
import com.google.common.io.Closeables;

public class GetHandler extends MethodHandler {
    private static final Logger log = LoggerFactory.getLogger(GetHandler.class);

    public GetHandler(WebdavRequestHandler requestHandler) {
        super(requestHandler);
    }

    FsFile file;

    @Override
    public void close() {
        super.close();

        if (file != null) {
            Closeables.closeQuietly(file);
            file = null;
        }
    }

    @Override
    public HttpObject doAction(FsPath fsPath) throws Exception {
        log.debug("GET {}", fsPath);
        WebdavRequest request = getRequest();

        // TODO: Handle ETAG??

        // If-Modified-Since Validation
        String ifModifiedSince = request.getHeader(HttpHeaders.Names.IF_MODIFIED_SINCE);
        if (!Strings.isNullOrEmpty(ifModifiedSince)) {
            Date ifModifiedSinceDate;
            try {
                ifModifiedSinceDate = HttpHeaders.getDateHeader(request.request, HttpHeaders.Names.IF_MODIFIED_SINCE);
            } catch (ParseException e) {
                throw new IllegalArgumentException();
            }

            // Only compare up to the second because the datetime format we send to the client
            // does not have milliseconds
            long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
            long fileLastModifiedSeconds = fsPath.getModified() / 1000;
            if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
                throw new WebdavResponseException(HttpResponseStatus.NOT_MODIFIED);
            }
        }

        HttpResponseStatus status = HttpResponseStatus.OK;
        long contentLength = -1;
        ChunkedInput<ByteBuf> content = null;

        if (fsPath.isFolder()) {
            // Empty
            contentLength = 0;
        } else {
            // TODO: Support range fetches
            file = getFsClient().openFile(fsPath);

            HttpFetchRange range = request.getRange();

            contentLength = fsPath.getLength();

            if (request.getMethod() == HttpMethod.GET) {
                if (range != null) {
                    status = HttpResponseStatus.PARTIAL_CONTENT;
                    if (range.getFrom() != null) {
                        long from = range.getFrom();
                        contentLength -= from;
                    }
                    if (range.getTo() != null) {
                        long to = range.getTo();
                        if ((to + 1) < fsPath.getLength()) {
                            contentLength -= fsPath.getLength() - (to + 1);
                        }
                    }
                    content = file.open(range.getFrom(), range.getTo());
                } else {
                    content = file.open();
                }
            }
        }

        HttpResponse response = buildResponseHeader(status);
        if (contentLength >= 0) {
            HttpHeaders.setContentLength(response, contentLength);
        }

        String mimeType = getMimeHelper().getMimeType(fsPath.getName());
        response.headers().set(HttpHeaders.Names.CONTENT_TYPE, mimeType);

        int httpCacheSeconds = 0;
        setDateAndCacheHeaders(fsPath, response, httpCacheSeconds);

        log.debug("Sending response to GET: {} {}", fsPath.getHref(), response);
        write(response);
        if (content != null) {
            while (!content.isEndOfInput()) {
                ByteBuf chunk = content.readChunk(requestHandler.ctx);
                if (chunk != null) {
                    write(new DefaultHttpContent(chunk));
                }
            }
        }
        return DefaultLastHttpContent.EMPTY_LAST_CONTENT;
    }

}
