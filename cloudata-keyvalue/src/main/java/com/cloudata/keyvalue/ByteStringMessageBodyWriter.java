package com.cloudata.keyvalue;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;

import com.google.protobuf.ByteString;

@Produces({ "application/octet-stream", "*/*" })
public final class ByteStringMessageBodyWriter implements MessageBodyWriter<ByteString> {

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return ByteString.class.isAssignableFrom(type);
    }

    @Override
    public long getSize(ByteString t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return t.size();
    }

    @Override
    public void writeTo(ByteString t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException,
            WebApplicationException {
        t.writeTo(entityStream);
    }

}