package com.cloudata.keyvalue;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class CloseOnRemoval<K, V extends Closeable> implements RemovalListener<K, V> {

    private static final Logger log = LoggerFactory.getLogger(CloseOnRemoval.class);

    @Override
    public void onRemoval(RemovalNotification<K, V> notification) {
        V value = notification.getValue();
        try {
            value.close();
        } catch (IOException e) {
            log.warn("Error while closing cache object", e);
            throw Throwables.propagate(e);
        }
    }

    public static <K, V extends Closeable> CloseOnRemoval<K, V> build() {
        return new CloseOnRemoval<K, V>();
    }
}
