package com.cloudata.files.locks;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import javax.inject.Singleton;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@Singleton
public class InMemoryLockService implements LockService {

    static class Lockable {
        final String key;
        final List<InMemoryLockToken> locks = Lists.newArrayList();

        public Lockable(String key) {
            this.key = key;
        }

        synchronized InMemoryLockToken lock() {
            if (!locks.isEmpty()) {
                return null;
            }

            String token = UUID.randomUUID().toString();
            InMemoryLockToken lock = new InMemoryLockToken(this, token);
            locks.add(lock);
            return lock;
        }

        synchronized InMemoryLockToken unlock(String lockToken) {
            for (int i = 0; i < locks.size(); i++) {
                InMemoryLockToken lock = locks.get(i);
                if (lockToken.equals(lock.getLockToken())) {
                    locks.remove(i);
                    return lock;
                }
            }
            return null;
        }

        synchronized InMemoryLockToken find(String lockToken) {
            for (int i = 0; i < locks.size(); i++) {
                InMemoryLockToken lock = locks.get(i);
                if (lockToken.equals(lock.getLockToken())) {
                    return lock;
                }
            }
            return null;
        }
    }

    /**
     * The lock implements the Java Lock. It is a lockable, not a held lock!
     * 
     */
    static class InMemoryLock implements CloudLock {
        final Lockable lockable;

        public InMemoryLock(Lockable lockable) {
            this.lockable = lockable;
        }

        @Override
        public void lock() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void unlock() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * A LockToken represents a hold on a lock.
     * 
     */
    static class InMemoryLockToken {
        final Lockable lock;
        final String token;

        public InMemoryLockToken(Lockable lock, String token) {
            this.lock = lock;
            this.token = token;
        }

        public String getLockToken() {
            return token;
        }
    }

    final Map<String, InMemoryLock> locks = Maps.newHashMap();

    @Override
    public InMemoryLock get(String key) {
        InMemoryLock lockset;
        synchronized (locks) {
            lockset = locks.get(key);

            if (lockset == null) {
                lockset = new InMemoryLock(new Lockable(key));
                locks.put(key, lockset);
            }
        }

        return lockset;
    }

    // @Override
    // public InMemoryLockToken lock(String uri) {
    // LockSet lockset;
    // synchronized (locks) {
    // lockset = locks.get(uri);
    //
    // if (lockset == null) {
    // lockset = new LockSet();
    // locks.put(uri, lockset);
    // }
    // }
    //
    // return lockset.lock();
    // }
    //
    // @Override
    // public InMemoryLock unlock(String uri, String lockToken) {
    // // TODO: Use findLock
    //
    // LockSet lockset;
    // synchronized (locks) {
    // lockset = locks.get(uri);
    // }
    //
    // if (lockset == null) {
    // return null;
    // }
    //
    // return lockset.unlock(lockToken);
    // }
    //
    // @Override
    // public InMemoryLock findLock(String uri, String lockToken) {
    // LockSet lockset;
    // synchronized (locks) {
    // lockset = locks.get(uri);
    // }
    //
    // if (lockset == null) {
    // return null;
    // }
    //
    // return lockset.find(lockToken);
    // }
}