package com.cloudata.objectstore;

import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.StorageType;

public class JCloudsFileInfo extends JCloudsObjectInfoBase {

    public JCloudsFileInfo(String container, BlobMetadata blobMetadata) {
        super(container, blobMetadata);
        assert blobMetadata.getType() == StorageType.BLOB;
    }

    @Override
    public long getLength() {
        return ((BlobMetadata) metadata).getContentMetadata().getContentLength();
    }

    @Override
    public boolean isSubdir() {
        return false;
    }

}
