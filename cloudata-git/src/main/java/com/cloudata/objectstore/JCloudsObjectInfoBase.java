package com.cloudata.objectstore;

import org.jclouds.blobstore.domain.StorageMetadata;

public abstract class JCloudsObjectInfoBase extends ObjectInfo {

    protected final String container;
    protected final StorageMetadata metadata;

    public JCloudsObjectInfoBase(String container, StorageMetadata metadata) {
        this.container = container;
        this.metadata = metadata;
    }

    @Override
    public String getPath() {
        return container + "/" + metadata.getName();
    }

}
