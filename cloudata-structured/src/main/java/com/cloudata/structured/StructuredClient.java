package com.cloudata.structured;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.clients.StreamingRecordsetBase;
import com.cloudata.structured.sql.value.ValueHolder;
import com.cloudata.util.ByteStringMessageBodyWriter;
import com.cloudata.util.GsonObjectMessageBodyHandler;
import com.cloudata.util.Hex;
import com.google.common.io.ByteStreams;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

public class StructuredClient {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(StructuredClient.class);

    static final Client CLIENT = buildClient();
    final String url;

    public StructuredClient(String url) {
        this.url = url;
    }

    private static Client buildClient() {
        ClientConfig config = new DefaultClientConfig();
        config.getClasses().add(ByteStringMessageBodyWriter.class);
        config.getClasses().add(GsonObjectMessageBodyHandler.class);
        Client client = Client.create(config);
        client.setFollowRedirects(true);
        return client;
    }

    public void put(long storeId, ByteString key, JsonObject value) throws Exception {
        ClientResponse response = CLIENT.resource(url).path(toUrlPath(storeId, key))
                .entity(value, MediaType.APPLICATION_JSON).post(ClientResponse.class);

        try {
            int status = response.getStatus();

            switch (status) {
            case 200:
                break;

            default:
                throw new IllegalStateException("Unexpected status: " + status);
            }
        } finally {
            response.close();
        }
    }

    public static class KeyValueJsonEntry {
        private final ByteString key;
        private final JsonElement value;

        public KeyValueJsonEntry(ByteString key, JsonElement value) {
            this.key = key;
            this.value = value;
        }

        public ByteString getKey() {
            return key;
        }

        public JsonElement getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "JsonElement [key=" + Hex.forDebug(key) + ", value=" + value + "]";
        }

    }

    public KeyValueJsonEntry readJson(long storeId, ByteString key) throws IOException {
        ClientResponse response = CLIENT.resource(url).path(toUrlPath(storeId, key))
                .accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

        try {
            int status = response.getStatus();

            switch (status) {
            case 200:
                break;

            case 404:
                return null;

            default:
                throw new IllegalStateException("Unexpected status: " + status);
            }

            InputStream is = response.getEntityInputStream();
            JsonElement value = new JsonParser().parse(new InputStreamReader(is));

            return new KeyValueJsonEntry(key, value);
        } finally {
            response.close();
        }
    }

    public KeyValueJsonRecordset queryJson(long storeId) throws IOException {
        ClientResponse response = CLIENT.resource(url).path(toUrlPath(storeId)).accept(MediaType.APPLICATION_JSON_TYPE)
                .get(ClientResponse.class);

        try {
            int status = response.getStatus();

            switch (status) {
            case 200:
                break;

            default:
                throw new IllegalStateException("Unexpected status: " + status);
            }

            KeyValueJsonRecordset records = new KeyValueJsonRecordset(response);
            response = null;
            return records;
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    public RowRecordset queryJson(long storeId, String sql) throws IOException {
        ClientResponse response = CLIENT.resource(url).path(toUrlPath(storeId) + "/sql").queryParam("sql", sql)
                .accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

        try {
            int status = response.getStatus();

            switch (status) {
            case 200:
                break;

            default:
                throw new IllegalStateException("Unexpected status: " + status);
            }

            RowRecordset records = new RowRecordset(response);
            response = null;
            return records;
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    public static class KeyValueJsonRecordset extends StreamingRecordsetBase<KeyValueJsonEntry> {
        private final DataInputStream dis;

        public KeyValueJsonRecordset(ClientResponse response) {
            super(response);

            InputStream is = response.getEntityInputStream();
            this.dis = new DataInputStream(is);
        }

        @Override
        protected KeyValueJsonEntry read() throws IOException {
            int keyLength = dis.readInt();
            if (keyLength == -1) {
                return null;
            }

            ByteString key = ByteString.readFrom(ByteStreams.limit(dis, keyLength));
            if (key.size() != keyLength) {
                throw new EOFException();
            }

            int valueLength = dis.readInt();
            JsonElement object = new JsonParser().parse(new InputStreamReader(ByteStreams.limit(dis, valueLength)));

            return new KeyValueJsonEntry(key, object);
        }
    }

    public static class RowRecordset extends StreamingRecordsetBase<RowEntry> {
        private final CodedInputStream cis;

        public RowRecordset(ClientResponse response) {
            super(response);

            InputStream is = response.getEntityInputStream();
            this.cis = CodedInputStream.newInstance(is);
        }

        @Override
        protected RowEntry read() throws IOException {
            int rowCount = cis.readRawVarint32();
            if (rowCount == -1) {
                return null;
            }

            ValueHolder[] values = new ValueHolder[rowCount];
            for (int i = 0; i < rowCount; i++) {
                values[i] = new ValueHolder();
            }

            for (int i = 0; i < rowCount; i++) {
                values[i].deserialize(cis);
            }

            return new RowEntry(values);
        }
    }

    public static class RowEntry {
        final ValueHolder[] values;

        public RowEntry(ValueHolder[] values) {
            this.values = values;
        }

        public int size() {
            return values.length;
        }

        public ValueHolder get(int i) {
            return values[i];
        }

        @Override
        public String toString() {
            return "RowEntry [values=" + Arrays.toString(values) + "]";
        }

    }

    private String toUrlPath(long storeId, ByteString key) {
        return Long.toString(storeId) + "/" + Hex.toHex(key);
    }

    private String toUrlPath(long storeId) {
        return Long.toString(storeId);
    }

    public void delete(long storeId, ByteString key) {
        ClientResponse response = CLIENT.resource(url).path(toUrlPath(storeId, key)).delete(ClientResponse.class);

        try {
            int status = response.getStatus();

            switch (status) {
            case 200:
            case 204:
                break;

            default:
                throw new IllegalStateException("Unexpected status: " + status);
            }
        } finally {
            response.close();
        }
    }
}
