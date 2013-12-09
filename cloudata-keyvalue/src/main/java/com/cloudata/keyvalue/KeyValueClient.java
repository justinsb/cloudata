package com.cloudata.keyvalue;

import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                throw new IllegalStateException();
            }
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
                throw new IllegalStateException();
            }

            InputStream is = response.getEntityInputStream();
            ByteString value = ByteString.readFrom(is);

            return new KeyValueEntry(key, value);
        } finally {
            response.close();
        }
    }

    private String toUrlPath(long storeId, ByteString key) {
        return Long.toString(storeId) + "/" + Hex.toHex(key);
    }
}
