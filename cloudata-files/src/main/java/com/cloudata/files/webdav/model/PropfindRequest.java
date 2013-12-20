package com.cloudata.files.webdav.model;

import java.io.IOException;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.cloudata.files.webdav.WebdavRequest;
import com.google.common.collect.Lists;

public class PropfindRequest {
    private final WebdavRequest request;
    private final Document doc;

    private final Element propfindElement;

    public PropfindRequest(WebdavRequest request) throws IOException {
        this.request = request;

        this.doc = request.getRequestDocument();
        // System.out.println(XmlHelper.toXml(doc));

        if (this.doc != null) {
            this.propfindElement = this.doc.getDocumentElement();
            String nodeName = propfindElement.getLocalName();
            if (!nodeName.equals("propfind")) {
                throw new IOException("Error parsing request - expected propfind");
            }
        } else {
            this.propfindElement = null;
        }
    }

    Element findPropElement() {
        if (propfindElement == null) {
            return null;
        }
        NodeList children = propfindElement.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node item = children.item(i);
            if (item.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            if (!item.getNodeName().equals("prop")) {
                continue;
            }

            if (item.getNamespaceURI().equals(WebdavXmlWriter.XMLNS_DAV)) {
                return (Element) item;
            }
        }
        return null;
    }

    public WebdavPropertyList getRequestedProperties() {
        Element prop = findPropElement();
        if (prop == null) {
            return WebdavPropertyList.WILDCARD;
        }

        List<WebdavProperty> properties = Lists.newArrayList();

        NodeList children = prop.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node item = children.item(i);
            if (item.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            properties.add(WebdavProperty.build(item.getNamespaceURI(), item.getNodeName()));
        }

        return new WebdavPropertyList(properties);
    }

}
