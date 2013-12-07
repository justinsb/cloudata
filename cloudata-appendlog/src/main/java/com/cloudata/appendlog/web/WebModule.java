package com.cloudata.appendlog.web;

import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

public class WebModule extends ServletModule {
    @Override
    protected void configureServlets() {
        bind(AppendLogEndpoint.class);
        bind(ByteBufferProvider.class);

        serve("/*").with(GuiceContainer.class);
    }
}
