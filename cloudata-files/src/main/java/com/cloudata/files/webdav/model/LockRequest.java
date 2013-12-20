package com.cloudata.files.webdav.model;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.cloudata.files.webdav.WebdavRequest;

public class LockRequest {
    private static final Logger log = LoggerFactory.getLogger(LockRequest.class);

    private final WebdavRequest request;
    private final Document doc;

    private final Element lockElement;

    public LockRequest(WebdavRequest request) throws IOException {
        this.request = request;

        this.doc = request.getRequestDocument();
        if (this.doc != null) {
            this.lockElement = this.doc.getDocumentElement();
            String nodeName = lockElement.getLocalName();
            if (!nodeName.equals("lockinfo")) {
                log.info("Error parsing XML: {}", XmlHelper.asString(doc));
                throw new IOException("Error parsing request - expected lockinfo");
            }
        } else {
            this.lockElement = null;
        }
    }

    @Override
    public String toString() {
        return XmlHelper.asString(doc);
    }

}
