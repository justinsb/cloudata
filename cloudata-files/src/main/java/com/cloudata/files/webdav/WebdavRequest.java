package com.cloudata.files.webdav;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.cloudata.files.fs.FsCredentials;
import com.cloudata.files.fs.PlaintextPasswordCredentials;
import com.cloudata.files.webdav.chunks.ChunkAccumulator;
import com.cloudata.files.webdav.chunks.SimpleChunkAccumulator;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.BaseEncoding;

public class WebdavRequest implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(WebdavRequest.class);

    final HttpRequest request;

    final ChunkAccumulator content;

    public WebdavRequest(HttpRequest request, ChunkAccumulator content) throws IOException {
        this.request = request;
        this.content = content;
    }

    public HttpMethod getMethod() {
        return request.getMethod();
    }

    public String getUri() {
        return request.getUri();
    }

    public boolean isKeepAlive() {
        return HttpHeaders.isKeepAlive(request);
    }

    public String getHeader(String name) {
        return request.headers().get(name);
    }

    public Document getRequestDocument() throws IOException {
        if (!hasContent()) {
            return null;
        }

        try (InputStream is = ((SimpleChunkAccumulator) content).open()) {
            // String xml = IoUtils.readAll(is);
            // System.out.println(xml);

            boolean namespaceAware = true;

            Document document = null;
            try {
                document = parseXmlDocument(is, namespaceAware);
            } catch (ParserConfigurationException e) {
                throw new IOException("Error parsing request", e);
            } catch (SAXException e) {
                throw new IOException("Error parsing request", e);
            }

            return document;
        }
    }

    public static Document parseXmlDocument(InputStream is, boolean namespaceAware)
            throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilder docBuilder = buildDocumentBuilder(namespaceAware);
        Document doc = docBuilder.parse(is);

        // normalize text representation
        doc.getDocumentElement().normalize();

        return doc;
    }

    private static DocumentBuilder buildDocumentBuilder(boolean namespaceAware) throws ParserConfigurationException {
        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        docBuilderFactory.setNamespaceAware(namespaceAware);
        DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
        return docBuilder;
    }

    public static final int DEPTH_INFINITY = -1;

    public int getDepth() {
        int depth = DEPTH_INFINITY;
        String depthHeader = request.headers().get("Depth");
        if (depthHeader != null) {
            if (depthHeader.equals("0")) {
                depth = 0;
            } else if (depthHeader.equals("1")) {
                depth = 1;
            } else if (depthHeader.equals("infinity")) {
                depth = DEPTH_INFINITY;
            } else {
                log.warn("Unknown depth; assuming infinity: " + depthHeader);
            }
        }
        return depth;
    }

    public boolean hasContent() {
        return content != null && !content.isEmpty();
    }

    public void addTrailingHeaders(LastHttpContent lastHttpContent) {
        request.headers().add(lastHttpContent.trailingHeaders());
    }

    public void addChunk(ByteBuf chunkContent) throws IOException {
        if (chunkContent != null && chunkContent.isReadable()) {
            content.append(chunkContent);
        }
    }

    public void endContent() throws IOException {
        content.end();
    }

    public ChunkAccumulator getContent() {
        return content;
    }

    @Override
    public void close() throws IOException {
        content.close();
    }

    public FsCredentials getCredentials() {
        String auth = request.headers().get("Authorization");
        if (Strings.isNullOrEmpty(auth)) {
            return null;
        }
        if (auth.startsWith("Basic ")) {
            try {
                String token = auth.substring(6);
                byte[] data = BaseEncoding.base64().decode(token);
                String authString = new String(data, Charsets.UTF_8);
                int colonIndex = authString.indexOf(':');
                if (colonIndex == -1) {
                    return null;
                }
                String username = authString.substring(0, colonIndex);
                String password = authString.substring(colonIndex + 1);
                return new PlaintextPasswordCredentials(username, password);
            } catch (Exception e) {
                log.warn("Error parsing BASIC authentication token", e);
                return null;
            }
        } else {
            log.warn("Unsupported authentication: " + auth);
            return null;
        }
    }

    public HttpFetchRange getRange() {
        String range = request.headers().get("Range");
        if (range == null) {
            return null;
        }
        return HttpFetchRange.parse(range);
    }

    @Override
    public String toString() {
        return "WebdavRequest [request=" + request + "]";
    }

}
