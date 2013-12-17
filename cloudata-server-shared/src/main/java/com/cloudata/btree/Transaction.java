package com.cloudata.btree;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.values.Value;

public abstract class Transaction implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Transaction.class);

    final PageStore pageStore;

    private final Lock lock;

    boolean releasedLock;

    public Transaction(PageStore pageStore, Lock lock) {
        this.pageStore = pageStore;
        this.lock = lock;
    }

    public abstract Page getPage(Page parent, int pageNumber);

    public void walk(Btree btree, ByteBuffer from, EntryListener listener) {
        Page rootPage = getRootPage(btree, false);
        if (rootPage == null) {
            log.info("No data; returning immediately from walk");
            return;
        }
        rootPage.walk(this, from, listener);
    }

    public Value get(Btree btree, ByteBuffer key) {
        GetEntryListener listener = new GetEntryListener(key);
        this.walk(btree, key, listener);

        Value value = listener.foundValue;

        if (value != null) {
            value = value.clone();
        }

        return value;
    }

    protected abstract Page getRootPage(Btree btree, boolean create);

    @Override
    public void close() {
        unlock();

        pageStore.finished(this);
    }

    protected void unlock() {
        synchronized (this) {
            if (lock != null && !releasedLock) {
                lock.unlock();
                releasedLock = true;
            }
        }
    }

}
