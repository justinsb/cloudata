package com.cloudata.files.locks;

import java.util.List;
import java.util.Map;
import java.util.UUID;

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

        synchronized InMemoryLockToken tryLock() {
            if (!locks.isEmpty()) {
                return null;
            }

            String token = UUID.randomUUID().toString();
            InMemoryLockToken lock = new InMemoryLockToken(this, token);
            locks.add(lock);
            return lock;
        }

        synchronized boolean unlock(String lockToken) {
            for (int i = 0; i < locks.size(); i++) {
                InMemoryLockToken lock = locks.get(i);
                if (lockToken.equals(lock.getTokenId())) {
                    locks.remove(i);
                    return true;
                }
            }
            return false;
        }

        synchronized InMemoryLockToken findLock(String lockToken) {
            for (int i = 0; i < locks.size(); i++) {
                InMemoryLockToken lock = locks.get(i);
                if (lockToken.equals(lock.getTokenId())) {
                    return lock;
                }
            }
            return null;
        }

    }

    // /**
    // * The lock implements the Java Lock. It is a lockable, not a held lock!
    // *
    // */
    // static class InMemoryLock implements CloudLock {
    // final Lockable lockable;
    //
    // public InMemoryLock(Lockable lockable) {
    // this.lockable = lockable;
    // }
    //
    // @Override
    // public void lock() {
    // throw new UnsupportedOperationException();
    // }
    //
    // @Override
    // public void lockInterruptibly() throws InterruptedException {
    // throw new UnsupportedOperationException();
    // }
    //
    // @Override
    // public boolean tryLock() {
    // throw new UnsupportedOperationException();
    // }
    //
    // @Override
    // public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    // throw new UnsupportedOperationException();
    // }
    //
    // @Override
    // public void unlock() {
    // throw new UnsupportedOperationException();
    // }
    //
    // @Override
    // public Condition newCondition() {
    // throw new UnsupportedOperationException();
    // }
    //
    // @Override
    // public CloudLockToken getLockToken() {
    // throw new UnsupportedOperationException();
    // }
    //
    // }

    /**
     * A LockToken represents a hold on a lock.
     * 
     */
    static class InMemoryLockToken implements CloudLockToken {
        final Lockable lock;
        final String tokenId;

        public InMemoryLockToken(Lockable lock, String tokenId) {
            this.lock = lock;
            this.tokenId = tokenId;
        }

        @Override
        public String getTokenId() {
            return tokenId;
        }
    }

    final Map<String, Lockable> locks = Maps.newHashMap();

    private Lockable getLockable(String key) {
        Lockable lockset;
        synchronized (locks) {
            lockset = locks.get(key);

            if (lockset == null) {
                lockset = new Lockable(key);
                locks.put(key, lockset);
            }
        }

        return lockset;
    }

    @Override
    public CloudLockToken tryLock(String key) {
        return getLockable(key).tryLock();
    }

    @Override
    public boolean unlock(String key, String lockToken) {
        return getLockable(key).unlock(lockToken);
    }

    @Override
    public CloudLockToken findLock(String key, String lockToken) {
        return getLockable(key).findLock(lockToken);
    }
}