package com.cloudata.keyvalue.btree;

public abstract class PageStore {

    public abstract Page fetchPage(Page parent, int pageNumber);

    /**
     * Writes the page to the PageStore (disk, usually)
     * 
     * @param page
     * @return the new page number
     */
    public abstract int writePage(Page page);

    public abstract int getRootPageId();

    public abstract void commitTransaction(int newRootPage);
}
