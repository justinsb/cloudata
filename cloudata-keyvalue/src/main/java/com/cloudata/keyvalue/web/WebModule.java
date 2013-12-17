package com.cloudata.keyvalue.web;

import com.cloudata.btree.BtreeQueryBodyWriter;
import com.cloudata.util.ByteBufferProvider;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

public class WebModule extends ServletModule {
    @Override
    protected void configureServlets() {
        bind(KeyValueEndpoint.class);

        bind(ByteBufferProvider.class);
        bind(BtreeQueryBodyWriter.class);

        serve("/*").with(GuiceContainer.class);
    }
}
