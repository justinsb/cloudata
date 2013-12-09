package com.cloudata.keyvalue.web;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import javax.inject.Singleton;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

@Provider
@Singleton
@Produces({ "application/octet-stream", "*/*" })
public final class ByteBufferProvider implements MessageBodyWriter<ByteBuffer> {

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return ByteBuffer.class.isAssignableFrom(type);
    }

    @Override
    public long getSize(ByteBuffer t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return t.remaining();
    }

    @Override
    public void writeTo(ByteBuffer t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException,
            WebApplicationException {
        WritableByteChannel channel = Channels.newChannel(entityStream);

        channel.write(t);
    }

}