package com.cloudata.files.fs;

import com.cloudata.files.FilesModel.InodeData;
import com.google.protobuf.ByteString;

public class FsPath extends Inode {
    final FsPath parent;
    private final FsVolume volume;

    public FsPath(FsVolume volume, FsPath parent, ByteString name, long inode, InodeData data) {
        super(name, inode, data);

        this.parent = parent;
        this.volume = volume;
    }

    public FsVolume getVolume() {
        return volume;
    }

    public String getHref() {
        StringBuilder sb = new StringBuilder();
        buildHref(sb);
        return sb.toString();
    }

    private void buildHref(StringBuilder sb) {
        if (parent != null) {
            parent.buildHref(sb);
        }

        if (parent != null) {
            // Don't append the root node name
            sb.append(this.getName());
        }

        if (this.isFolder()) {
            sb.append('/');
        }
    }

}
