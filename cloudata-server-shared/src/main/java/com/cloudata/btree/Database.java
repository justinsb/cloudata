package com.cloudata.btree;

import java.io.File;
import java.io.IOException;

public class Database {
    final PageStore pageStore;
    final TransactionTracker transactionTracker;

    private Database(PageStore pageStore) throws IOException {
        this.pageStore = pageStore;

        MasterPage latest = pageStore.findLatestMasterPage();
        this.transactionTracker = new TransactionTracker(this, latest);
    }

    public static Database build(File file) throws IOException {
        // PageStore pageStore = MmapPageStore.build(file);
        PageStore pageStore = CachingPageStore.build(file);
        return new Database(pageStore);
    }

    public PageStore getPageStore() {
        return pageStore;
    }
}
