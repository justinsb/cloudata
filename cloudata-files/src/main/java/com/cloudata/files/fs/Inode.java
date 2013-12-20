package com.cloudata.files.fs;

import java.util.List;

import com.cloudata.files.FilesModel.ChunkData;
import com.cloudata.files.FilesModel.InodeData;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class Inode {

    static final int S_IFMT = 0170000;
    public static final int S_IFDIR = 0040000;

    final long id;
    final InodeData data;
    final ByteString name;
    private String nameUtf8;

    public Inode(ByteString name, long id, InodeData data) {
        this.name = name;
        this.id = id;
        Preconditions.checkNotNull(data);
        this.data = data;
    }

    public List<ChunkData> getChunkList() {
        assert !isFolder();
        return data.getChunkList();
    }

    public boolean isFolder() {
        return (data.getMode() & S_IFMT) == S_IFDIR;
    }

    public long getId() {
        return id;
    }

    public long getModified() {
        return data.getModifiedTime();
    }

    public long getCreated() {
        return data.getCreateTime();
    }

    public long getLength() {
        return data.getLength();
    }

    public ByteString getNameBytes() {
        return name;
    }

    public String getName() {
        if (nameUtf8 == null) {
            nameUtf8 = name.toStringUtf8();
        }
        return nameUtf8;
    }
}
