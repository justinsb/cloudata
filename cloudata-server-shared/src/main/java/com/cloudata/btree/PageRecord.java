package com.cloudata.btree;

import com.cloudata.freemap.SpaceMapEntry;

public class PageRecord {
    public final Page page;
    public final SpaceMapEntry space;

    public PageRecord(Page page, SpaceMapEntry space) {
        this.page = page;
        this.space = space;
    }
}
