package com.cloudata.keyvalue;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.CloseableClientResponse;
import com.cloudata.clients.StreamingRecordsetBase;
import com.cloudata.util.ByteStringMessageBodyWriter;
import com.cloudata.util.Hex;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.ClientFilter;

public class KeyValueClient {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(KeyValueClient.class);

    static final Client CLIENT = buildClient();
    final String startUrl;

    String leader;

    public KeyValueClient(String url) {
        this.startUrl = url;
        this.leader = url;
    }

    private static Client buildClient() {
        ClientConfig config = new DefaultClientConfig();
        config.getClasses().add(ByteStringMessageBodyWriter.class);
        Client client = Client.create(config);
        client.setFollowRedirects(true);
        client.addFilter(new ClientFilter() {

            @Override
            public ClientResponse handle(ClientRequest cr) throws ClientHandlerException {
                log.debug("Making request {}", cr.getURI());

                ClientResponse response = getNext().handle(cr);

                return response;
            }

        });
        return client;
    }

    abstract class Retry<T> {
        protected abstract void call() throws IOException;

        boolean done = false;
        private T value;

        public T apply() throws IOException {
            do {
                call();
                if (done) {
                    return value;
                }
            } while (!done);

            throw new IOException();
        }

        public void done(T value) {
            this.done = true;
            this.value = value;

        }

        public void handleRedirect(ClientResponse response) {
            URI location = response.getLocation();
            if (location == null) {
                throw new IllegalStateException("Unexpected redirect without location");
            }
            log.info("Following leader redirect from {} to {}", leader, location);
            leader = location.toString();
        }
    }

    public void put(final long storeId, final ByteString key, final ByteString value) throws Exception {
        Retry<Void> retry = new Retry<Void>() {
            @Override
            protected void call() {
                ClientResponse response = CLIENT.resource(leader).path(toUrlPath(storeId, key))
                        .entity(value, MediaType.APPLICATION_OCTET_STREAM_TYPE).post(ClientResponse.class);

                try {
                    int status = response.getStatus();

                    switch (status) {
                    case 200:
                        done(null);
                        break;

                    case 303:
                        handleRedirect(response);
                        break;

                    default:
                        throw new IllegalStateException("Unexpected status: " + status);
                    }
                } finally {
                    response.close();
                }
            }
        };

        retry.apply();
    }

    public KeyValueEntry increment(final long storeId, final ByteString key) throws Exception {
        Retry<KeyValueEntry> retry = new Retry<KeyValueEntry>() {
            @Override
            protected void call() throws IOException {
                ClientResponse response = CLIENT.resource(leader).path(toUrlPath(storeId, key))
                        .queryParam("action", "increment").post(ClientResponse.class);

                try {
                    int status = response.getStatus();

                    switch (status) {
                    case 200:
                        InputStream is = response.getEntityInputStream();
                        ByteString value = ByteString.readFrom(is);

                        done(new KeyValueEntry(key, value));
                        break;

                    case 303:
                        handleRedirect(response);
                        break;

                    default:
                        throw new IllegalStateException("Unexpected status: " + status);
                    }
                } finally {
                    response.close();
                }
            }
        };

        return retry.apply();
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

    public KeyValueEntry read(final long storeId, final ByteString key) throws IOException {
        Retry<KeyValueEntry> retry = new Retry<KeyValueEntry>() {
            @Override
            protected void call() throws IOException {
                ClientResponse response = CLIENT.resource(leader).path(toUrlPath(storeId, key))
                        .get(ClientResponse.class);

                try {
                    int status = response.getStatus();

                    switch (status) {
                    case 200:
                        InputStream is = response.getEntityInputStream();
                        ByteString value = ByteString.readFrom(is);

                        done(new KeyValueEntry(key, value));
                        break;

                    case 303:
                        handleRedirect(response);
                        break;

                    case 404:
                        done(null);
                        break;

                    default:
                        throw new IllegalStateException("Unexpected status: " + status);
                    }
                } finally {
                    response.close();
                }
            }
        };

        return retry.apply();
    }

    public KeyValueRecordset query(final long storeId) throws IOException {
        Retry<KeyValueRecordset> retry = new Retry<KeyValueRecordset>() {
            @Override
            protected void call() throws IOException {
                ClientResponse response = CLIENT.resource(leader).path(toUrlPath(storeId)).get(ClientResponse.class);

                try {
                    int status = response.getStatus();

                    switch (status) {
                    case 200:
                        KeyValueRecordset records = new KeyValueRecordset(response);
                        response = null;
                        done(records);
                        break;

                    case 303:
                        handleRedirect(response);
                        break;

                    default:
                        throw new IllegalStateException("Unexpected status: " + status);
                    }
                } finally {
                    if (response != null) {
                        response.close();
                    }
                }
            }
        };

        return retry.apply();
    }

    static class KeyValueRecordset extends StreamingRecordsetBase<KeyValueEntry> {
        private final DataInputStream dis;

        public KeyValueRecordset(ClientResponse response) {
            super(new CloseableClientResponse(response));
            InputStream is = response.getEntityInputStream();
            this.dis = new DataInputStream(is);
        }

        @Override
        protected KeyValueEntry read() throws IOException {
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

    public void delete(final long storeId, final ByteString key) throws IOException {
        Retry<Void> retry = new Retry<Void>() {
            @Override
            protected void call() throws IOException {
                ClientResponse response = CLIENT.resource(leader).path(toUrlPath(storeId, key))
                        .delete(ClientResponse.class);

                try {
                    int status = response.getStatus();

                    switch (status) {
                    case 200:
                    case 204:
                        done(null);
                        break;

                    case 303:
                        handleRedirect(response);
                        break;

                    default:
                        throw new IllegalStateException("Unexpected status: " + status);
                    }
                } finally {
                    response.close();
                }
            }
        };

        retry.apply();
    }
}
