package com.cloudata.files.webdav.model;

import javax.xml.stream.XMLStreamException;

public interface XmlSerializable {

    void write(WebdavXmlWriter webdavXmlWriter) throws XMLStreamException;

}
