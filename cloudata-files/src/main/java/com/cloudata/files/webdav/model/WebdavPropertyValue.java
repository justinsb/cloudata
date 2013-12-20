package com.cloudata.files.webdav.model;

public class WebdavPropertyValue {
    // TODO: Special handling of serialization
    public static final WebdavPropertyValue RESOURCETYPE_COLLECTION = new WebdavPropertyValue(
            WebdavProperty.RESOURCETYPE, "collection");

    final WebdavProperty property;
    final String value;

    public WebdavPropertyValue(WebdavProperty property, String value) {
        this.property = property;
        this.value = value;
    }

    public WebdavProperty getProperty() {
        return property;
    }

    public String getValue() {
        return value;
    }

}
