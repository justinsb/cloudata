package com.cloudata.keyvalue;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.clients.StreamingRecordsetBase;
import com.cloudata.util.ByteStringMessageBodyWriter;
import com.cloudata.util.Hex;
import com.google.common.io.ByteStreams;
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

    static class KeyValueRecordset extends StreamingRecordsetBase<KeyValueEntry> {
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
