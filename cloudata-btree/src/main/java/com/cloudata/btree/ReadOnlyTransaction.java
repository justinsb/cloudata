package com.cloudata.btree;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.Futures;

public class ReadOnlyTransaction extends Transaction {
    private static final Logger log = LoggerFactory.getLogger(ReadOnlyTransaction.class);

    final int rootPageId;
    final long snapshotTransactionId;
    final int transactionPageId;

    final Cache<Integer, PageRecord> pages;

    public ReadOnlyTransaction(Database db, int rootPageId, long snapshotTransactionId, int transactionPageId) {
        super(db, null);
        this.rootPageId = rootPageId;
        this.snapshotTransactionId = snapshotTransactionId;
        this.transactionPageId = transactionPageId;

        this.pages = CacheBuilder.newBuilder().removalListener(new RemovalListener<Integer, PageRecord>() {
            @Override
            public void onRemoval(RemovalNotification<Integer, PageRecord> notification) {
                PageRecord pageRecord = notification.getValue();
                pageRecord.release();
            }
        }).maximumSize(16).build();
    }

    @Override
    public Page getPage(final Btree btree, final Page parent, final int pageNumber) {
        // We need the page to hold on to the buffer. Otherwise this simply doesn't work right...
        log.warn("ReadOnly transaction page retention logic is incorrect");

        PageRecord pageRecord;
        try {
            pageRecord = pages.get(pageNumber, new Callable<PageRecord>() {
                @Override
                public PageRecord call() throws IOException {
                    PageRecord pageRecord = Futures.get(db.pageStore.fetchPage(btree, parent, pageNumber),
                            IOException.class);
                    assert pageRecord.refCnt() == 1;
                    return pageRecord;
                }
            });
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }

        Page page = pageRecord.page;
        return page;
    }

    @Override
    protected Page getRootPage(Btree btree, boolean create) {
        if (rootPageId == 0) {
            if (!create) {
                return null;
            }
            throw new IllegalStateException();
        }

        return getPage(btree, null, rootPageId);
    }

    public long getSnapshotTransactionId() {
        return snapshotTransactionId;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void close() {
        pages.invalidateAll();

        super.close();
    }

}
