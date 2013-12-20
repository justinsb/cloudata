package com.cloudata.files.webdav.model;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class WebdavPropertyList {
    private static final Logger log = LoggerFactory.getLogger(WebdavPropertyList.class);

    public static final WebdavPropertyList WILDCARD = new WebdavPropertyList(null);

    final List<WebdavProperty> properties;

    final Map<WebdavProperty, Integer> index;

    public WebdavPropertyList(List<WebdavProperty> properties) {
        super();
        this.properties = properties;
        this.index = buildIndex(properties);
    }

    public boolean contains(WebdavProperty property) {
        if (properties == null) {
            return true;
        }
        return index.get(property) != null;
    }

    private static Map<WebdavProperty, Integer> buildIndex(List<WebdavProperty> properties) {
        if (properties == null) {
            return null;
        }

        Map<WebdavProperty, Integer> index = Maps.newHashMap();
        for (int i = 0; i < properties.size(); i++) {
            index.put(properties.get(i), i);
        }
        return index;
    }

    public Integer getIndex(WebdavProperty property) {
        if (properties == null) {
            throw new IllegalArgumentException();
        }
        return index.get(property);
    }

    public int size() {
        return properties.size();
    }

    public boolean isWildcard() {
        return properties == null;
    }

    public WebdavProperty get(int i) {
        return properties.get(i);
    }
}
