package com.cloudata.files.webdav;

import javax.activation.MimetypesFileTypeMap;
import javax.inject.Singleton;

@Singleton
public class MimeHelper {
    final MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();

    public String getMimeType(String fileName) {
        return mimeTypesMap.getContentType(fileName);
    }
}
