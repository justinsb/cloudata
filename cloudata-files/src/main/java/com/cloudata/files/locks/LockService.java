package com.cloudata.files.locks;

public interface LockService {

    CloudLockToken tryLock(String key);

    boolean unlock(String key, String lockToken);

}
