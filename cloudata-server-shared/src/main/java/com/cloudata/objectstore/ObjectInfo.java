package com.cloudata.objectstore;

public abstract class ObjectInfo {

    public abstract long getLength();

    public abstract String getPath();

    public abstract boolean isSubdir();

}
