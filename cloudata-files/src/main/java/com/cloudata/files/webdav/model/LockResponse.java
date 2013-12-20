package com.cloudata.files.webdav.model;

import javax.xml.stream.XMLStreamException;

public class LockResponse implements XmlSerializable {
    private final String href;

    public LockResponse(String href) {
        this.href = href;
    }

    @Override
    public void write(WebdavXmlWriter writer) throws XMLStreamException {
        writer.writeStartDavElement("prop");
        writer.writeStartDavElement("lockdiscovery");
        writer.writeStartDavElement("activelock");

        writer.writeStartDavElement("locktoken");
        writer.writeSimpleDavElement("href", href);
        writer.writeEndElement(); // locktoken

        writer.writeEndElement(); // activelock
        writer.writeEndElement(); // lockdiscovery
        writer.writeEndElement(); // response
    }

}
