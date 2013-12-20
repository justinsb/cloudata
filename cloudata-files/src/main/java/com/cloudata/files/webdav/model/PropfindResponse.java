package com.cloudata.files.webdav.model;

import java.util.BitSet;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class PropfindResponse implements XmlSerializable {
    private static final Logger log = LoggerFactory.getLogger(PropfindResponse.class);

    final WebdavPropertyList requestedProperties;

    final List<Response> responses = Lists.newArrayList();

    public PropfindResponse(WebdavPropertyList requestedProperties) {
        this.requestedProperties = requestedProperties;
    }

    public class Response {
        final String href;
        final List<WebdavPropertyValue> properties = Lists.newArrayList();

        Response(String href) {
            super();
            this.href = href;
        }

        public void addProperty(WebdavPropertyValue property) {
            properties.add(property);
        }

        public void write(WebdavXmlWriter writer) throws XMLStreamException {
            writer.writeStartDavElement("response");
            writer.writeSimpleDavElement("href", href);

            // Properties that were found (status code 200)
            writer.writeStartDavElement("propstat");
            BitSet found = null;
            if (!requestedProperties.isWildcard()) {
                found = new BitSet(requestedProperties.size());
            }
            writer.writeStartDavElement("prop");
            for (WebdavPropertyValue property : properties) {
                writer.writeProperty(property);
                if (found != null) {
                    Integer index = requestedProperties.getIndex(property.property);
                    if (index != null) {
                        found.set(index);
                    }
                }
            }
            writer.writeEndElement(); // prop
            writer.writeStatus(200);
            writer.writeEndElement(); // propstat

            // Any properties that were not found
            if (found != null && found.cardinality() != requestedProperties.size()) {
                writer.writeStartDavElement("propstat");
                writer.writeStartDavElement("prop");

                for (int i = 0; i < requestedProperties.size(); i++) {
                    if (found.get(i)) {
                        continue;
                    }

                    WebdavProperty requestedProperty = requestedProperties.get(i);
                    writer.writeEmptyProperty(requestedProperty);
                }
                writer.writeEndElement(); // prop
                writer.writeStatus(404);
                writer.writeEndElement(); // propstat
            }

            writer.writeEndElement(); // response
        }

        public void addProperty(WebdavProperty property, String value) {
            addProperty(new WebdavPropertyValue(property, value));
        }
    }

    @Override
    public void write(WebdavXmlWriter writer) throws XMLStreamException {
        writer.writeStartDavElement("multistatus");
        for (Response response : responses) {
            response.write(writer);
        }
        writer.writeEndElement(); // multistatus
    }

    public Response addResponse(String href) {
        Response response = new Response(href);
        responses.add(response);
        return response;
    }

}
