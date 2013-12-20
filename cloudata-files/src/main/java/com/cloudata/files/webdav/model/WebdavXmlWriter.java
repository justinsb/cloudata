package com.cloudata.files.webdav.model;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

public class WebdavXmlWriter {

    public static final String PREFIX_DAV = "d";
    public static final String XMLNS_DAV = "DAV:";

    final XMLStreamWriter writer;

    public WebdavXmlWriter(XMLStreamWriter writer) throws XMLStreamException {
        super();
        this.writer = writer;
    }

    public void writeStartDavElement(String localname) throws XMLStreamException {
        // writer.writeStartElement(PREFIX_DAV, localname, XMLNS_DAV);
        String prefix = writer.getPrefix(XMLNS_DAV);
        writer.writeStartElement(PREFIX_DAV, localname, XMLNS_DAV);
        if (prefix == null) {
            writer.setPrefix(PREFIX_DAV, XMLNS_DAV);
            writer.writeNamespace(PREFIX_DAV, XMLNS_DAV);
        }
    }

    int autoPrefix = 0;

    public void writeStartElement(String namespace, String localname) throws XMLStreamException {
        String prefix = writer.getPrefix(namespace);
        boolean bind = false;

        if (prefix == null) {
            bind = true;

            if (namespace.equals(XMLNS_DAV)) {
                prefix = PREFIX_DAV;
            } else {
                prefix = "ns" + (++autoPrefix);
            }
        }

        writer.writeStartElement(prefix, localname, namespace);
        if (bind) {
            writer.setPrefix(prefix, namespace);
            writer.writeNamespace(prefix, namespace);
        }
    }

    public void writeEmptyElement(String namespace, String localname) throws XMLStreamException {
        writeStartElement(namespace, localname);
        writeEndElement();
    }

    public void writeEndElement() throws XMLStreamException {
        writer.writeEndElement();
    }

    public void writeSimpleDavElement(String localname, String value) throws XMLStreamException {
        writeStartDavElement(localname);
        if (value != null) {
            writer.writeCharacters(value);
        }
        writer.writeEndElement();
    }

    public void writeStatus(int code) throws XMLStreamException {
        writeStartDavElement("status");

        switch (code) {
        case 200:
            writer.writeCharacters("HTTP/1.1 200 OK");
            break;
        case 404:
            writer.writeCharacters("HTTP/1.1 404 Not Found");
            break;
        default:
            throw new IllegalArgumentException();
        }

        writer.writeEndElement();
    }

    public void writeEmptyProperty(WebdavProperty property) throws XMLStreamException {
        writeEmptyElement(property.getNamespace(), property.getName());
    }

    public void writeProperty(WebdavPropertyValue propertyValue) throws XMLStreamException {
        WebdavProperty property = propertyValue.getProperty();
        writer.writeStartElement(property.getNamespace(), property.getName());

        String value = propertyValue.getValue();

        if (property == WebdavProperty.RESOURCETYPE) {
            // Special case
            if (value.equals("collection")) {
                // writer.writeEmptyElement(PREFIX_DAV, "collection", XMLNS_DAV);
                writer.writeEmptyElement(XMLNS_DAV, "collection");
            } else {
                throw new IllegalArgumentException();
            }
        } else {
            writer.writeCharacters(value);
        }
        writer.writeEndElement();
    }
}
