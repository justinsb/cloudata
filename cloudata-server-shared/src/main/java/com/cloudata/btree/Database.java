package com.cloudata.btree;

public class Database {
    final PageStore pageStore;
    final TransactionTracker transactionTracker;

    Database(PageStore pageStore) {
        this.pageStore = pageStore;

        MasterPage latest = pageStore.findLatestMasterPage();
        this.transactionTracker = new TransactionTracker(this, latest);
    }

}
