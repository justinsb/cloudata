package com.cloudata.files.webdav.model;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * WebdavProperty are immutable singletons, to enable fast comparisons
 * 
 * @author justinsb
 * 
 */
public class WebdavProperty {
    private static final Logger log = LoggerFactory.getLogger(WebdavProperty.class);

    final static Map<String, WebdavProperty> properties = Maps.newHashMap();

    public static final String DAV = WebdavXmlWriter.XMLNS_DAV;

    public static final WebdavProperty RESOURCETYPE = build(DAV, "resourcetype");

    public static final WebdavProperty GETLASTMODIFIED = build(DAV, "getlastmodified");
    public static final WebdavProperty GETCONTENTLENGTH = build(DAV, "getcontentlength");
    public static final WebdavProperty CREATIONDATE = build(DAV, "creationdate");
    public static final WebdavProperty GETETAG = build(DAV, "getetag");

    private final String namespace;
    private final String name;

    private WebdavProperty(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    public static synchronized WebdavProperty build(String namespace, String name) {
        String key = namespace + ":" + name;
        WebdavProperty property = properties.get(key);
        if (property == null) {
            property = new WebdavProperty(namespace, name);
            properties.put(key, property);
        }
        return property;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "WebdavProperty [namespace=" + namespace + ", name=" + name + "]";
    }

}
