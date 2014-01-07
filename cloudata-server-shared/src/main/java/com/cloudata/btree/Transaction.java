package com.cloudata.btree;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudata.btree.operation.BtreeOperation;
import com.cloudata.btree.operation.ComplexOperation;
import com.cloudata.btree.operation.RowOperation;
import com.cloudata.values.Value;
import com.google.common.base.Preconditions;

public abstract class Transaction implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Transaction.class);

    final Database db;

    private final Lock lock;

    boolean releasedLock;

    public Transaction(Database db, Lock lock) {
        this.db = db;
        this.lock = lock;
    }

    public abstract Page getPage(Btree btree, Page parent, int pageNumber);

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

    public <V> void doAction(Btree btree, BtreeOperation<V> operation) {
        if (operation instanceof RowOperation) {
            RowOperation<V> rowOperation = (RowOperation<V>) operation;
            boolean readOnly = this.isReadOnly();

            if (readOnly) {
                Preconditions.checkState(operation.isReadOnly());
            }

            Page rootPage = getRootPage(btree, !readOnly);
            if (rootPage == null) {
                assert readOnly;
                // Skip - nothing to read
            } else {
                rootPage.doAction(this, rowOperation.getKey(), rowOperation);
            }
        } else if (operation instanceof ComplexOperation) {
            ((ComplexOperation) operation).doAction(btree, this);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public abstract boolean isReadOnly();

    protected abstract Page getRootPage(Btree btree, boolean create);

    @Override
    public void close() {
        unlock();

        db.transactionTracker.finished(this);
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
