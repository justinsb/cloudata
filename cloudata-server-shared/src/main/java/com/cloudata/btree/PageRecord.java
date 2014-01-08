package com.cloudata.btree;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

import com.cloudata.freemap.SpaceMapEntry;

/**
 * PageRecord associates a page with its backing space and also retains a pointer to the buffer
 * 
 * TODO: Should we merge this into Page? It was originally just used as a structured return from the PageCache.
 * 
 */
public class PageRecord extends AbstractReferenceCounted {
    public final Page page;
    public final SpaceMapEntry space;
    private final ReferenceCounted ref;

    public PageRecord(Page page, SpaceMapEntry space, ReferenceCounted ref) {
        this.page = page;
        this.space = space;
        this.ref = ref;

        if (ref != null) {
            ref.retain();
        }
    }

    @Override
    protected void deallocate() {
        if (this.ref != null) {
            this.ref.release();
        }
    }

}
