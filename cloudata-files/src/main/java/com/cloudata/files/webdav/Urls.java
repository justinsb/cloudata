package com.cloudata.files.webdav;

public class Urls {

    public static String trimLastSlash(String path) {
        int len = path.length();
        if (len == 0) {
            return path;
        }
        char lastChar = path.charAt(len - 1);
        if (lastChar != '/') {
            return path;
        }

        return path.substring(0, len - 1);
    }

    public static String getParentPath(String path) {
        path = trimLastSlash(path);

        int lastSlashIndex = path.lastIndexOf('/');
        if (lastSlashIndex == -1) {
            return null;
        }
        return path.substring(0, lastSlashIndex + 1);
    }

    public static String getLastPathComponent(String path) {
        path = trimLastSlash(path);

        int lastSlashIndex = path.lastIndexOf('/');
        if (lastSlashIndex == -1) {
            return null;
        }
        return path.substring(lastSlashIndex + 1);
    }

}
