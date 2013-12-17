package com.cloudata.keyvalue;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.util.Hex;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

public class KeyValueClient {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(KeyValueClient.class);

    static final Client CLIENT = buildClient();
    final String url;

    public KeyValueClient(String url) {
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

    public void put(long storeId, ByteString key, ByteString value) throws Exception {
        ClientResponse response = CLIENT.resource(url).path(toUrlPath(storeId, key))
                .entity(value, MediaType.APPLICATION_OCTET_STREAM_TYPE).post(ClientResponse.class);

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

    public KeyValueEntry increment(long storeId, ByteString key) throws Exception {
        ClientResponse response = CLIENT.resource(url).path(toUrlPath(storeId, key)).queryParam("action", "increment")
                .post(ClientResponse.class);

        try {
            int status = response.getStatus();

            switch (status) {
            case 200:
                break;

            default:
                throw new IllegalStateException("Unexpected status: " + status);
            }

            InputStream is = response.getEntityInputStream();
            ByteString value = ByteString.readFrom(is);

            return new KeyValueEntry(key, value);
        } finally {
            response.close();
        }
    }

    public static class KeyValueEntry {
        private final ByteString key;
        private final ByteString value;

        public KeyValueEntry(ByteString key, ByteString value) {
            this.key = key;
            this.value = value;
        }

        public ByteString getKey() {
            return key;
        }

        public ByteString getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "KeyValueEntry [key=" + Hex.forDebug(key) + ", value=" + Hex.forDebug(value) + "]";
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

    public KeyValueEntry read(long storeId, ByteString key) throws IOException {
        ClientResponse response = CLIENT.resource(url).path(toUrlPath(storeId, key)).get(ClientResponse.class);

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
            ByteString value = ByteString.readFrom(is);

            return new KeyValueEntry(key, value);
        } finally {
            response.close();
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

    public KeyValueRecordset query(long storeId) throws IOException {
        ClientResponse response = CLIENT.resource(url).path(toUrlPath(storeId)).get(ClientResponse.class);

        try {
            int status = response.getStatus();

            switch (status) {
            case 200:
                break;

            default:
                throw new IllegalStateException("Unexpected status: " + status);
            }

            KeyValueRecordset records = new KeyValueRecordset(response);
            response = null;
            return records;
        } finally {
            if (response != null) {
                response.close();
            }
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

    static abstract class KeyValueRecordsetBase<V> implements AutoCloseable, Iterable<V> {
        final ClientResponse response;

        boolean read;

        public KeyValueRecordsetBase(ClientResponse response) {
            this.response = response;
        }

        @Override
        public void close() {
            response.close();
        }

        @Override
        public Iterator<V> iterator() {
            if (read) {
                throw new IllegalStateException();
            }
            read = true;
            InputStream is = response.getEntityInputStream();

            final DataInputStream dis = new DataInputStream(is);

            return new Iterator<V>() {
                V next;
                boolean done;

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public V next() {
                    ensureHaveNext();

                    if (next != null) {
                        V ret = next;
                        next = null;
                        return ret;
                    } else {
                        throw new NoSuchElementException();
                    }
                }

                @Override
                public boolean hasNext() {
                    ensureHaveNext();

                    return next != null;
                }

                private void ensureHaveNext() {
                    if (next == null) {
                        if (!done) {
                            try {
                                next = read(dis);
                            } catch (IOException e) {
                                throw Throwables.propagate(e);
                            }
                            if (next == null) {
                                done = true;
                            }
                        }
                    }
                }

            };

        }

        protected abstract V read(DataInputStream dis) throws IOException;
    }

    static class KeyValueRecordset extends KeyValueRecordsetBase<KeyValueEntry> {
        public KeyValueRecordset(ClientResponse response) {
            super(response);
        }

        @Override
        protected KeyValueEntry read(DataInputStream dis) throws IOException {
            int keyLength = dis.readInt();
            if (keyLength == -1) {
                return null;
            }

            ByteString key = ByteString.readFrom(ByteStreams.limit(dis, keyLength));
            if (key.size() != keyLength) {
                throw new EOFException();
            }

            int valueLength = dis.readInt();
            ByteString value = ByteString.readFrom(ByteStreams.limit(dis, valueLength));
            if (value.size() != valueLength) {
                throw new EOFException();
            }

            return new KeyValueEntry(key, value);
        }
    }

    static class KeyValueJsonRecordset extends KeyValueRecordsetBase<KeyValueJsonEntry> {
        public KeyValueJsonRecordset(ClientResponse response) {
            super(response);
        }

        @Override
        protected KeyValueJsonEntry read(DataInputStream dis) throws IOException {
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
