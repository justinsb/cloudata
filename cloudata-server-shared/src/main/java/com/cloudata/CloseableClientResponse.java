package com.cloudata;

import java.io.Closeable;
import java.io.IOException;

import com.sun.jersey.api.client.ClientResponse;

public class CloseableClientResponse implements Closeable {

    private final ClientResponse response;

    public CloseableClientResponse(ClientResponse response) {
        this.response = response;
    }

    @Override
    public void close() throws IOException {
        response.close();
    }

}
