package com.cloudata.structured.web;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;

import javax.inject.Singleton;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.structured.sql.RowsetListener;
import com.cloudata.structured.sql.SqlStatement;
import com.cloudata.structured.sql.value.ValueHolder;
import com.google.protobuf.CodedOutputStream;

@Provider
@Singleton
@Produces({ "application/octet-stream", "*/*" })
public final class SqlStatementBodyWriter implements MessageBodyWriter<SqlStatement> {

    private static final Logger log = LoggerFactory.getLogger(SqlStatement.class);

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return SqlStatement.class.isAssignableFrom(type);
    }

    @Override
    public long getSize(SqlStatement query, Class<?> type, Type genericType, Annotation[] annotations,
            MediaType mediaType) {
        return -1;
    }

    @Override
    public void writeTo(SqlStatement statement, Class<?> type, Type genericType, Annotation[] annotations,
            MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
            throws IOException, WebApplicationException {
        // CodedOutputStream includes a write buffer, so we don't need another one
        final CodedOutputStream os = CodedOutputStream.newInstance(entityStream);

        RowsetListener listener = new RowsetListener() {
            @Override
            public void beginRows() {

            }

            @Override
            public boolean gotRow(ValueHolder[] expressionValues) throws Exception {
                os.writeInt32NoTag(expressionValues.length);

                for (int i = 0; i < expressionValues.length; i++) {
                    expressionValues[i].serializeTo(os);
                }

                return true;
            }

            @Override
            public void endRows() throws IOException {
                os.writeInt32NoTag(-1);
                os.flush();
            }
        };

        statement.execute(listener);
    }

    static void copy(ByteBuffer src, OutputStream os, byte[] buffer) throws IOException {
        while (true) {
            int n = src.remaining();
            if (n == 0) {
                break;
            }

            n = Math.min(buffer.length, n);

            src.get(buffer, 0, n);

            os.write(buffer, 0, n);
        }
    }

}