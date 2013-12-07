package com.cloudata.appendlog;

import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.appendlog.web.AppendLogEndpoint;
import com.google.common.io.ByteStreams;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class AppendLogClient {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(AppendLogClient.class);

    final Client client;
    final String url;

    public AppendLogClient(Client client, String url) {
        this.client = client;
        this.url = url;
    }

    public void append(long logId, byte[] data) throws Exception {
        ClientResponse response = client.resource(url).path(Long.toString(logId))
                .entity(data, MediaType.APPLICATION_OCTET_STREAM_TYPE).post(ClientResponse.class);
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

    public static class AppendLogEntry {

        private final byte[] data;
        private final long nextPos;

        public AppendLogEntry(byte[] data, long nextPos) {
            this.data = data;
            this.nextPos = nextPos;
        }

        public byte[] getData() throws IOException {
            return data;
        }

        public long getNextPosition() {
            return nextPos;
        }

    }

    public AppendLogEntry read(long logId, long position) throws IOException {
        ClientResponse clientResponse = client.resource(url)
                .path(Long.toString(logId) + "/" + Long.toHexString(position)).get(ClientResponse.class);

        // System.out.println("Response was " + clientResponse);

        try {
            int status = clientResponse.getStatus();

            switch (status) {
            case 200:
                break;

            case 404:
                return null;

            default:
                throw new IllegalStateException();
            }

            String next = clientResponse.getHeaders().getFirst(AppendLogEndpoint.HEADER_NEXT);
            if (next == null) {
                throw new IllegalStateException();
            }

            long nextPos = Long.valueOf(next, 16);

            InputStream is = clientResponse.getEntityInputStream();
            byte[] data = ByteStreams.toByteArray(is);

            return new AppendLogEntry(data, nextPos);
        } finally {
            clientResponse.close();
        }
    }
}
