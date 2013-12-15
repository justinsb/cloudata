package com.cloudata.objectstore;

import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;

public class JCloudsDirInfo extends JCloudsObjectInfoBase {

    public JCloudsDirInfo(String container, StorageMetadata metadata) {
        super(container, metadata);
        assert metadata.getType() == StorageType.FOLDER;
    }

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public boolean isSubdir() {
        return true;
    }

}
